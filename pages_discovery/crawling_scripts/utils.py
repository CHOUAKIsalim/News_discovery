import fasttext
import fasttext.util
from sklearn.metrics.pairwise import cosine_similarity
from params import DIR_KW_FASTTEXT

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