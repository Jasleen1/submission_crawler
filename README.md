To use the script, replace the API_KEY placeholder on line 9 with an API key from Semantic Scholar (you can request it here: https://www.semanticscholar.org/product/api#api-key-form).

Then add your keywords to keywords.txt and your desired venues to venues.txt. You may need to check the names Semantic Scholar uses for the venues. 

Usage
```
python venue_crawler.py \
  --venues_file venues.txt \
  --year_from 2005 \
  --limit 100 \
  --batch_size 100 \
  --keywords_file keywords.txt \
  --model relevance_head.joblib \
  --out_csv harvested_papers.csv
```
