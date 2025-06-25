import os
import sqlite3
import re
import zipfile
import sys

def word_count(s: str) -> int:
    """Counts the number of words in a space-separated string."""
    return len(s.split())

arabic_diacritics = re.compile(r'[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED]')

def remove_diacritics(text):
    return re.sub(arabic_diacritics, '', text)


def main():
    """
    Main function to process the Hadith database.
    """
    # Construct the path to the database file relative to the script's location
    db_path = "data/hadiths.db"

    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    source_con = sqlite3.connect(db_path)
    dest_con = sqlite3.connect(":memory:")

    source_con.backup(dest_con)
    source_con.close()

    dest_con.row_factory = sqlite3.Row
    cursor = dest_con.cursor()

    # Prepare the update statement
    update_sql = (
        'UPDATE hadiths SET body_en=?, text_en=?, text=?, chain=?, body=? '
        'WHERE bookId=? AND num=? AND chain_en IS NOT NULL'
    )

    print('Processing: split hadith chain and body')
    
    # --- Data Retrieval & Processing Loop ---
    cursor.execute('SELECT * FROM hadiths LIMIT 10')
    rows = cursor.fetchall()

    for row in rows:
        # Create a mutable dictionary from the immutable row object
        row_dict = dict(row)

        # --- English Text Normalization ---
        text_en = row_dict.get('text_en') or ''
        text_en
        if text_en.startswith('"'):
            text_en = text_en.strip('"')
        text_en = re.sub(r'"{2,}', '"', text_en)
        row_dict['text_en'] = text_en
        
        # Use a new key for the English body to avoid conflict
        row_dict['body_en'] = text_en

        # --- Arabic Text Normalization ---
        text = row_dict.get('text') or ''
        if text:
            text = re.sub(r'[:"\'،۔ـ\-\.,]', '', text)
            # Combine multiple replacements into one regex for efficiency
            honorifics = r'رضى الله عنها|رضى الله عنهما|رضى الله عنهم|رضى الله عنه'
            text = re.sub(honorifics, '', text)
            text = text.replace('صلى الله عليه وسلم', '')

            text = re.sub(r'\s+', ' ', text).strip()
        row_dict['text'] = text

        # --- Heuristic-Based Splitting ---
        # Create a simplified version of the text for analysis
        text_marked = text
        # 1. Strip diacritics
        text_marked = remove_diacritics(text_marked)
        
        # 2. Mark common narration keywords with a ~
        chain_keywords = [
            'حدثنا', 'حدثني', 'حدثناه', 'حدثه', 'ثنا',
            'أخبرنا', 'أخبرناه', 'أخبرني', 'أخبره',
            'سمعت', 'سمعنا', 'سمعناه', 'سمع',
            'عن', 'عنه', 'عنها',
            'يبلغ به',
            'أنه', 'أن', 'أنها',
            'قال', 'قالت'
        ]
        # Regex to find keywords, optionally preceded by 'و' (and)
        pattern = re.compile(r'\bو?(' + '|'.join(chain_keywords) + r')\b')
        text_marked = pattern.sub('~ \\1', text_marked)
        text_marked = re.sub(r'\s+', ' ', text_marked).strip()
        text_marked

        # 3. Find the start of the body
        body_marked = ''
        chain_delims = text_marked.split('~')
        
        # Filter out empty strings that may result from split
        chain_delims = [s.strip() for s in chain_delims if s.strip()]

        if chain_delims:
            chain_toks_word_count = [word_count(tok) for tok in chain_delims]
            for j, delim in enumerate(chain_delims):
                # Heuristic 1: Body starts with mention of the Prophet
                if re.search(r'(نبي|رسول)', delim):
                    body_marked = '~'.join(chain_delims[j:])
                    break
                # Heuristic 2: A long segment not containing 'ibn' is likely the body
                elif chain_toks_word_count[j] > 7 and not re.search(r' (بن|ابن) ', delim):
                    body_marked = '~'.join(chain_delims[j:])
                    break
            
            # Fallback: a'ssume the last segment is the body
            if not body_marked:
                body_marked = chain_delims[-1]
        
        body_marked = body_marked.replace('~', ' ').strip()

        # --- Apply Split to Original Text ---
        text_toks = text.split()
        body_marked_toks = body_marked.split()

        row_dict['body'] = ''
        if not text_toks or not body_marked_toks or len(text_toks) <= len(body_marked_toks):
            row_dict['chain'] = ''
            row_dict['body'] = text
        else:
            diff = len(text_toks) - len(body_marked_toks)
            # Reconstruct chain and body from the original token list
            row_dict['chain'] = ' '.join(text_toks[:diff]).strip()
            row_dict['body'] = ' '.join(text_toks[diff:]).strip()

        # --- Database Update ---
        params = (
            row_dict['body_en'], row_dict['text_en'], row_dict['text'],
            row_dict['chain'], row_dict['body'],
            row_dict['bookId'], row_dict['num']
        )
        for param in params:
            print(param)
        import ipdb; ipdb.set_trace(context = 10) 
        cursor.execute(update_sql, params)
        print(f"Processed hadith {row_dict['bookId']}:{row_dict['num']}")

    # Commit all the changes to the in-memory database
    dest_con.commit()


if __name__ == '__main__':
    main()

