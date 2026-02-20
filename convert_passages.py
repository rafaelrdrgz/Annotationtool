
#converts JSONL passages file to app compatible JSON format
import json
import re
import nltk
from pathlib import Path

# download NLTK punkt tokenizer (only needed once)
nltk.download('punkt_tab', quiet=True)

#paths
input_file = Path(__file__).parent / "data" / "annotations" / "filtered_passages_deduplicated.jsonl"
output_file = Path(__file__).parent / "data" / "passages.json"

def split_into_sentences(text):
    #split text into sentences with NLTK then reattach citations to preceeding sentence
    raw_sentences = nltk.tokenize.sent_tokenize(text)

    # NLTK puts citation brackets at start of next sentence sometimes
    # eg "[116] However ..." should be "...loneliness.[116]" + "However ..."
    merged = []
    for sent in raw_sentences:
        #if sentence is JUST citation brackets attach to previous
        if merged and re.fullmatch(r'[\[\]\d\s]+', sent.strip()):
            merged[-1] = merged[-1] + ' ' + sent
        else:
            #check for citations with square brackets in passage like [117]
            match = re.match(r'^((?:\[\d+\]\s*)+)(.*)', sent)
            if match and merged and match.group(2).strip():
                citations = match.group(1).rstrip()
                remainder = match.group(2).strip()
                merged[-1] = merged[-1] + ' ' + citations
                merged.append(remainder)
            else:
                merged.append(sent)

    return merged
# read JSONL and convert
passages = []
errors = []

print(f"Reading from: {input_file}")
with open(input_file, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, 1):
        if not line.strip():
            continue

        try:
            item = json.loads(line)

            #split text into sentences
            text = item['text']
            sentences = split_into_sentences(text)

            # create passage in app format
            passage = {
                'id': item['id'],
                "text": text,
                'sentences': sentences,
                "source": item.get("source_name", 'Unknown'),
                'article_title': item.get('article_title', 'Untitled'),
                'date': 'N/A',  #not available in source
                "word_count": item.get("word_count"),
                'article_url': item.get('article_url'),
                'score': item.get('score'),
                "priority": item.get("priority", 'MEDIUM')
            }
            passages.append(passage)

        except json.JSONDecodeError as e:
            errors.append(f"Line {line_num}: {e}")
            continue
        except Exception as e:
            errors.append(f"Line {line_num}: Unexpected error: {e}")
            continue

print(f"\nConversion complete:")
print(f"  Converted {len(passages)} passages")
if errors:
    print(f"  {len(errors)} errors")
    for err in errors[:5]:  #show first 5
        print(f"    - {err}")

#save as JSON
print(f"\nSaving to: {output_file}")
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(passages, f, indent=2, ensure_ascii=False)

# show sample
if passages:
    sample = passages[0]
    print(f"\nSample passage:")
    print(f"  ID: {sample['id']}")
    print(f"  Source: {sample['source']}")
    print(f"  Sentences: {len(sample['sentences'])}")
    print(f"  First sentence: {sample['sentences'][0][:80]}...")

print(f"\nTotal passages available: {len(passages)}")
print(f"Ready for production use!")
