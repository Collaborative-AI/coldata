import re
import string


def clean_text(text):
    # Remove non-printable characters (except spaces)
    text = ''.join(c if c.isprintable() else ' ' for c in text)

    # Normalize line breaks and spaces (e.g., convert newlines to space)
    text = re.sub(r'[\r\n\t]+', ' ', text)  # Replace newlines, tabs, etc., with spaces

    # Normalize whitespace (collapse multiple spaces to one)
    text = re.sub(r'\s+', ' ', text)

    # Trim leading and trailing spaces
    text = text.strip()

    return text


def join_content(content_list):
    # Function to add punctuation if missing
    def add_punctuation(s):
        if s and s[-1] not in string.punctuation:
            return s + "."
        return s

    content_list = [clean_text(s) for s in content_list]
    if not content_list:
        return ""

    # If the list has more than one element, ensure each element ends with punctuation
    if len(content_list) > 1:
        content_list = [add_punctuation(s) for s in content_list if len(s) > 0]
    content = " ".join(content_list)
    return content
