import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
from huggingface_hub import snapshot_download


class Embedding:
    def __init__(self, model_name='sentence-transformers/all-mpnet-base-v2',
                 snapshot_folder='output', device='cpu', max_length=512, normalize_embeddings=False):
        self.model_name = model_name
        self.device = device
        self.max_length = max_length
        self.normalize_embeddings = normalize_embeddings
        self.snapshot_folder = snapshot_folder

        self.model_path = snapshot_download(repo_id=self.model_name, cache_dir=self.snapshot_folder)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        self.model = AutoModel.from_pretrained(self.model_path).to(self.device)
        self.model.eval()

    def embed_documents(self, texts):
        with torch.no_grad():
            encoded = self.tokenizer(
                texts, padding=True, truncation=True, return_tensors='pt', max_length=self.max_length,
            )
            encoded = {k: v.to(self.device) for k, v in encoded.items()}
            model_output = self.model(**encoded)

            if hasattr(model_output, 'pooler_output') and model_output.pooler_output is not None:
                pooled = model_output.pooler_output
            else:
                pooled = self._fallback_mean_pooling(model_output, encoded['attention_mask'])

            if self.normalize_embeddings:
                pooled = F.normalize(pooled, p=2, dim=1)

        embeddigns = pooled.cpu().numpy()

        return embeddigns

    def get_sentence_embedding_dimension(self):
        return self.model.config.hidden_size

    def _fallback_mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output.last_hidden_state  # (B, T, D)
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
        sum_mask = input_mask_expanded.sum(dim=1).clamp(min=1e-9)
        return sum_embeddings / sum_mask
