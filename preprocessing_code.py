import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from knowledge_graph_utils import propagate_item_removal_to_kg

"""
Pre-processing the data
"""
ml1m_path = "data/ml1m"

ml1m_users_df = pd.read_csv(f'{ml1m_path}/users.dat', sep="::", names=["UserID","Gender","Age","Occupation","Zip-code"], header=None)
print(ml1m_users_df.head(5))
print(f"Unique Users: {len(ml1m_users_df)}")

ml1m_movies_df = pd.read_csv(f'{ml1m_path}/movies.dat', sep="::", names=["movie_id", "movie_name", "genre"], header=None, encoding='latin-1')
print(ml1m_movies_df.head(5))
print(f"Unique Products: {len(ml1m_movies_df)}")

ml1m_ratings_df = pd.read_csv(f'{ml1m_path}/ratings.dat', sep="::", names=["user_id", "movie_id", "rating", "timestamp"], header=None)
print(ml1m_ratings_df.head(5))
print(f"Unique interactions: {len(ml1m_ratings_df)}")

entities_df = pd.read_csv(f'{ml1m_path}/kg/e_map.dat', sep="\t", names=["entity_id", "entity_url"])
print(entities_df.head(5))

movies_to_kg_df = pd.read_csv(f'{ml1m_path}/kg/i2kg_map.tsv', sep="\t", names=["dataset_id", "movie_name", "entity_url"])
print(movies_to_kg_df.head(5))
print(f"Items mapped in the KG: {movies_to_kg_df.shape[0]}")

kg_df = pd.read_csv(f'{ml1m_path}/kg/kg.dat', sep="\t")
print(kg_df.head(5))
print(f"Number of triplets: {kg_df.shape[0]}")

relations_df = pd.read_csv(f'{ml1m_path}/kg/r_map.dat', sep="\t", names=["relation_id", "relation_url"])
print(relations_df)

print(f"Items in the original dataset: {ml1m_movies_df.shape[0]}")
print(f"Items correctly mapped in the KG: {movies_to_kg_df.shape[0]}")

number_of_movies = ml1m_movies_df.shape[0]
ml1m_movies_df = ml1m_movies_df[ml1m_movies_df['movie_id'].isin(movies_to_kg_df.dataset_id)]
ml1m_movies_df.reset_index()
print(ml1m_movies_df.head(5))
print(f"Number of rows removed due to missing links with KG: {number_of_movies - ml1m_movies_df.shape[0]}")

movies_to_kg_df = pd.merge(movies_to_kg_df, entities_df, on=["entity_url"])
print(movies_to_kg_df.head(5))
print(f"Correctly mapped items: {movies_to_kg_df.shape[0]}")

print(f"Movies before: {ml1m_movies_df.shape[0]}")
movies_to_kg_df = movies_to_kg_df[movies_to_kg_df.entity_id.isin(entities_df.entity_id)]
print(f"Number of rows removed due to missing entity data in KG: {movies_to_kg_df.shape[0]}")

number_of_ratings = ml1m_ratings_df.shape[0]
ml1m_ratings_df = ml1m_ratings_df[ml1m_ratings_df['movie_id'].isin(movies_to_kg_df.dataset_id)]
ml1m_ratings_df.reset_index()
print(ml1m_ratings_df.head(5))
print(f"Number of rows removed due to interaction with removed movie: {number_of_ratings - ml1m_ratings_df.shape[0]}")

counts_col_user = ml1m_ratings_df.groupby("user_id")["user_id"].transform(len)
counts_col_movies = ml1m_ratings_df.groupby("movie_id")["movie_id"].transform(len)
print(counts_col_user.head(5))

k_user, k_movie = 5, 5
mask_user = counts_col_user >= k_user
mask_movies = counts_col_movies >= k_movie
print(mask_user.head(5))

print(f"Number of ratings before: {ml1m_ratings_df.shape[0]}")
ml1m_ratings_df = ml1m_ratings_df[mask_user & mask_movies]
print(f"Number of ratings after: {ml1m_ratings_df.shape[0]}")
print(ml1m_ratings_df.head(5))

print(f"Number of users before threshold discarding (k={k_user}): {ml1m_users_df.shape[0]}")
ml1m_users_df = ml1m_users_df[ml1m_users_df.UserID.isin(ml1m_ratings_df.user_id.unique())]
print(f"Number of users after threshold discarding (k={k_user}): {ml1m_users_df.shape[0]}")

print(f"Number of items before threshold discarding (k={k_movie}): {ml1m_movies_df.shape[0]}")
ml1m_movies_df = ml1m_movies_df[ml1m_movies_df.movie_id.isin(ml1m_ratings_df.movie_id.unique())]
print(f"Number of items after threshold discarding (k={k_movie}): {ml1m_movies_df.shape[0]}")



"""
processing data
"""

movies_to_kg_df, entities_df, kg_df = propagate_item_removal_to_kg(ml1m_movies_df, movies_to_kg_df, entities_df, kg_df)

ml1m_preprocessed_path = 'data/ml1m/preprocessed'

ml1m_users_df = ml1m_users_df.drop(["Gender", "Age", "Occupation", "Zip-code"], axis=1)
print(ml1m_users_df.head(5))

ml1m_users_df.insert(0, 'new_id', range(ml1m_users_df.shape[0])) #Create a new incremental ID
print(ml1m_users_df.head(5))

ml1m_users_df.to_csv(f'{ml1m_preprocessed_path}/users.txt', header=["new_id", "raw_dataset_id"], index=False, sep='\t', mode='w+')

user_id2new_id = dict(zip(ml1m_users_df["UserID"], ml1m_users_df.new_id))

#Drop attributes
ml1m_movies_df = ml1m_movies_df.drop(["movie_name", "genre"], axis=1)
#Add new_id column
ml1m_movies_df.insert(0, 'new_id', range(ml1m_movies_df.shape[0])) #Create a new incremental ID
#Print
print(ml1m_movies_df.head(5))
print(ml1m_movies_df.shape[0])
#Save
ml1m_movies_df.to_csv(f'{ml1m_preprocessed_path}/products.txt', header=["new_id", "raw_dataset_id"], index=False, sep='\t', mode='w+')

movie_id2new_id = dict(zip(ml1m_movies_df["movie_id"], ml1m_movies_df.new_id))

ml1m_ratings_df["user_id"] = ml1m_ratings_df['user_id'].map(user_id2new_id)
ml1m_ratings_df["movie_id"] = ml1m_ratings_df['movie_id'].map(movie_id2new_id)
print(ml1m_ratings_df.head(5))

#Save ratings
ml1m_ratings_df.to_csv(f'{ml1m_preprocessed_path}/ratings.txt', header=["uid", "pid", "rating", "timestamp"], index=False, sep='\t', mode='w+')

"""
processing kg
"""

print(movies_to_kg_df.head(5))
print(f"Number of movies correctly mapped: {movies_to_kg_df.shape[0]}")

mask = kg_df['entity_tail'].isin(movies_to_kg_df.entity_id) \
        & ~kg_df['entity_head'].isin(movies_to_kg_df.entity_id)
kg_df.loc[mask, ['entity_head', 'entity_tail']] = \
    (kg_df.loc[mask, ['entity_tail', 'entity_head']].values)

n_of_triplets = kg_df.shape[0]
kg_df = kg_df[(kg_df['entity_head'].isin(movies_to_kg_df.entity_id) & ~kg_df['entity_tail'].isin(movies_to_kg_df.entity_id))]
print(kg_df.head(5))
print(f"Number of triplets before: {n_of_triplets}")
print(f"Number of triplets after: {kg_df.shape[0]}")

print(len(kg_df.relation.unique()))

v = kg_df[['relation']]
n_of_triplets = kg_df.shape[0]
kg_df = kg_df[v.replace(v.apply(pd.Series.value_counts)).gt(300).all(1)]
print(kg_df.head(5))
print(f"Number of triplets before: {n_of_triplets}")
print(f"Number of triplets after: {kg_df.shape[0]}")

print(len(kg_df.relation.unique()))

print(relations_df)
relations_df = relations_df[relations_df['relation_id'].isin(kg_df.relation.unique())]
relations_df.reset_index()
print(relations_df)

relations_df = relations_df[(relations_df['relation_id'] != 13) & (relations_df['relation_id'] != 8)]
print(relations_df) 

print(f"Triplets before: {kg_df.shape[0]}")
kg_df = kg_df[kg_df.relation.isin(relations_df.relation_id)]
print(f"Triplets after: {kg_df.shape[0]}")

print(f"Entities before: {entities_df.shape[0]}")
entities_df = entities_df[entities_df.entity_id.isin(kg_df.entity_head) | entities_df.entity_id.isin(kg_df.entity_tail)]
print(f"Entities after: {entities_df.shape[0]}")

ml1m_kg_preprocessed_path = 'data/ml1m/preprocessed/'

print(relations_df)
relations_df.to_csv(f'{ml1m_kg_preprocessed_path}/r_map.txt', header=["relation_id", "relation_url"], index=False, sep='\t', mode='w+')

print(entities_df.head(5))
entities_df.to_csv(f'{ml1m_kg_preprocessed_path}/e_map.txt', header=["entity_id", "entity_url"], index=False, sep='\t', mode='w+')

print(movies_to_kg_df.head(5))
movies_to_kg_df.to_csv(f'{ml1m_kg_preprocessed_path}/i2kg_map.txt', header=["dataset_id", "movie_name", 'entity_url', 'entity_id'], index=False, sep='\t', mode='w+')

print(kg_df.head(5))
kg_df.to_csv(f'{ml1m_kg_preprocessed_path}/kg_final.txt', header=["entity_head", "entity_tail", 'relation'], index=False, sep='\t', mode='w+')


