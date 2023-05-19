{% macro create_udf_sentiment_analysis() %}

create or replace function {{target.schema}}.udf_sentiment_analysis(text STRING, output INTEGER)
returns STRING
language PYTHON
runtime_version = 3.8
packages = ('pytorch','transformers','gensim')
handler = 'sentiment_analysis_py'
as

$$

from transformers import pipeline
from gensim.parsing.preprocessing import remove_stopwords
import string

model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
-- model = 'nlptown/bert-base-multilingual-uncased-sentiment'

def sentiment_analysis(text, model_id, output='score'):
    
    -- text pre-processing
    text = str(remove_stopwords(text).translate(str.maketrans('', '', string.punctuation)))    
    
    -- Classifier
    classifier = pipeline("sentiment-analysis",model=model_id)
    results = classifier(text)

    if output=='label':
        return results[0].get('label')
    else:
        return results[0].get('score')

$$

{% endmacro %}
