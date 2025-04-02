import fasttext
import fasttext.util
from sklearn.metrics.pairwise import cosine_similarity
from params import DIR_KW_FASTTEXT
from os import path
import pandas as pd

def get_bert_embedding(input_text_list):
    fasttext.FastText.eprint = lambda *args,**kwargs: None
    model = fasttext.load_model(DIR_KW_FASTTEXT+'cc.ro.300.bin')

    embeddings = []
    # Get BERT embeddings
    for text in input_text_list:
        embeddings.append(model.get_sentence_vector(text.replace('\n', ' ')))
    return embeddings

def attribute_posts_to_keywords(posts, keywords, logger):
    posts_to_keywords = {}
    logger.info("Embedding posts descriptions...")
    posts_embeddings = get_bert_embedding([post['video_description'] for post in posts])
    logger.info("Embedding posts keywords...")
    keywords_embeddings = get_bert_embedding(keywords)

    similarity_matrix = cosine_similarity(posts_embeddings, keywords_embeddings)

    for i in range(len(posts)):
        keyword_index = similarity_matrix[i].argmax()
        try:
            posts_to_keywords[keywords[keyword_index]].append(i)
        except KeyError:
            posts_to_keywords[keywords[keyword_index]] = [i]
    
    return posts_to_keywords  

def append_to_csv(df, desired_columns, filename):
    if not path.exists(filename):
        df_template = pd.DataFrame(columns=desired_columns)
        df_template.to_csv(filename, index=False)
    df = df.reindex(columns=desired_columns)
    df_file = pd.read_csv(filename)
    df = pd.concat([df_file, df], ignore_index=True)
    df.drop_duplicates(subset=['id'], inplace=True)
    #df.to_csv(filename[:-4]+'test.csv', mode='w', header=False, index=False)
    df.stack().str.replace('\n', '\\n', regex=True).unstack()
    df.to_csv(filename, mode='w', header=True, index=False, columns=desired_columns)