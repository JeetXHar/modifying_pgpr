import csv
import os
from collections import defaultdict
import argparse
from utils import ensure_dir
import gzip
import random

random.seed(64)





ML1M = 'ml1m'
LFM1M = 'lfm1m'
CELL = 'cellphones'

"""
ENTITIES
"""
#ML1M ENTITIES
MOVIE = 'movie'
ACTOR = 'actor'
DIRECTOR = 'director'
PRODUCTION_COMPANY = 'production_company'
EDITOR = 'editor'
WRITTER = 'writter'
CINEMATOGRAPHER = 'cinematographer'
COMPOSER = 'composer'
COUNTRY = 'country'
AWARD = 'award'

#LASTFM ENTITIES
SONG = 'song'
ARTIST = 'artist'
ENGINEER = 'engineer'
PRODUCER = 'producer'

#COMMON ENTITIES
USER = 'user'
CATEGORY = 'category'
PRODUCT = 'product'

RELATION_LIST = {
    ML1M: {
        0: "http://dbpedia.org/ontology/cinematography",
        1: "http://dbpedia.org/property/productionCompanies",
        2: "http://dbpedia.org/property/composer",
        3: "http://purl.org/dc/terms/subject",
        4: "http://dbpedia.org/ontology/openingFilm",
        5: "http://www.w3.org/2000/01/rdf-schema",
        6: "http://dbpedia.org/property/story",
        7: "http://dbpedia.org/ontology/series",
        8: "http://www.w3.org/1999/02/22-rdf-syntax-ns",
        9: "http://dbpedia.org/ontology/basedOn",
        10: "http://dbpedia.org/ontology/starring",
        11: "http://dbpedia.org/ontology/country",
        12: "http://dbpedia.org/ontology/wikiPageWikiLink",
        13: "http://purl.org/linguistics/gold/hypernym",
        14: "http://dbpedia.org/ontology/editing",
        15: "http://dbpedia.org/property/producers",
        16: "http://dbpedia.org/property/allWriting",
        17: "http://dbpedia.org/property/notableWork",
        18: "http://dbpedia.org/ontology/director",
        19: "http://dbpedia.org/ontology/award",
    },
    LFM1M: {
        0: "http://rdf.freebase.com/ns/common.topic.notable_types",
        1: "http://rdf.freebase.com/ns/music.recording.releases",
        2: "http://rdf.freebase.com/ns/music.recording.artist",
        3: "http://rdf.freebase.com/ns/music.recording.engineer",
        4: "http://rdf.freebase.com/ns/music.recording.producer",
        5: "http://rdf.freebase.com/ns/music.recording.canonical_version",
        6: "http://rdf.freebase.com/ns/music.recording.song",
        7: "http://rdf.freebase.com/ns/music.single.versions",
        8: "http://rdf.freebase.com/ns/music.recording.featured_artists",
    },
    CELL: {
        0: "belong_to",
        1: "also_buy_related_product",
        2: "also_buy_product",
        3: "produced_by_company",
        4: "also_view_related_product",
        5: "also_view_product",
    }
}
relation_name2entity_name = {
    ML1M: {
            "cinematographer_p_ci": 'cinematographer',
            "production_company_p_pr" :'production_company',
            "composer_p_co":'composer',
            "category_p_ca":'category',
            "actor_p_ac":'actor',
            "country_p_co":'country',
            "wikipage_p_wi":'wikipage',
            "editor_p_ed":'editor',
            "producer_p_pr":'producer',
            "writter_p_wr": 'writter',
            "director_p_di":'director',
        },
    LFM1M: {
        "category_p_ca": "category",
        "related_product_p_re": "related_product",
        "artist_p_ar": "artist",
        "engineer_p_en": "engineer",
        "producer_p_pr": "producer",
        "featured_artist_p_fe": "featured_artist",
    },
    CELL: {
        "category_p_ca": "category",
        "also_buy_related_product_p_re": "related_product",
        "also_buy_product_p_pr": "product",
        "brand_p_br": "brand",
        "also_view_related_product_p_re": "related_product",
        "also_view_product_p_pr": "product",
    }

}
relation_to_entity = {
    ML1M: {
        "http://dbpedia.org/ontology/cinematography": 'cinematographer',
        "http://dbpedia.org/property/productionCompanies": 'production_company',
        "http://dbpedia.org/property/composer": 'composer',
        "http://purl.org/dc/terms/subject": 'category',
        "http://dbpedia.org/ontology/starring": 'actor',
        "http://dbpedia.org/ontology/country": 'country',
        "http://dbpedia.org/ontology/wikiPageWikiLink": 'wikipage',
        "http://dbpedia.org/ontology/editing": 'editor',
        "http://dbpedia.org/property/producers": 'producer',
        "http://dbpedia.org/property/allWriting": 'writter',
        "http://dbpedia.org/ontology/director": 'director',
    },
    LFM1M: {
        "http://rdf.freebase.com/ns/common.topic.notable_types": "category",
        "http://rdf.freebase.com/ns/music.recording.releases": "related_product",
        "http://rdf.freebase.com/ns/music.recording.artist": "artist",
        "http://rdf.freebase.com/ns/music.recording.engineer": "engineer",
        "http://rdf.freebase.com/ns/music.recording.producer": "producer",
        "http://rdf.freebase.com/ns/music.recording.featured_artists": "featured_artist",
    },
    CELL: {
        "category": "category",
        "also_buy_related_product": "related_product",
        "also_buy_product": "product",
        "brand": "brand",
        "also_view_product": "product",
        "also_view_related_product": "related_product",
    }
}

relation_id2plain_name = {
    ML1M: {
        "0" : "cinematography_by",
        "1" : "produced_by_company",
        "2" : "composed_by",
        "3" : "belong_to",
        "10": "starred_by",
        "11": "produced_in",
        "12": "related_to",
        "14": "edited_by",
        "15": "produced_by_producer",
        "16": "wrote_by",
        "18": "directed_by",
    },
    LFM1M: {
        "0": "category",
        "1": "related_product",
        "2": "artist",
        "3": "engineer",
        "4": "producer",
        "5": "featured_artist",
    },
    CELL: {
        "0": "category",
        "1": "also_buy_related_product",
        "2": "related_product",
        "3": "brand",
        "4": "also_view_related_product",
        "5": "related_product"
    }
}

def write_time_based_train_test_split(dataset_name, model_name, train_size, valid_size=0, ratings_pid2local_id = {}, ratings_uid2global_id = {}):
    input_folder = f'data/{dataset_name}/preprocessed/'
    input_folder_kg = f'data/{dataset_name}/preprocessed/'
    output_folder = f'data/{dataset_name}/preprocessed/{model_name}/'

    ensure_dir(output_folder)

    uid2pids_timestamp_tuple = defaultdict(list)
    with open(input_folder + 'ratings.txt', 'r') as ratings_file: #uid	pid	rating	timestamp
        reader = csv.reader(ratings_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            k, pid, rating, timestamp = row
            uid2pids_timestamp_tuple[k].append([pid, int(timestamp)])
    ratings_file.close()

    for k in uid2pids_timestamp_tuple.keys():
        uid2pids_timestamp_tuple[k].sort(key=lambda x: x[1])

    train_file = gzip.open(output_folder + 'train.txt.gz', 'wt')
    writer_train = csv.writer(train_file, delimiter="\t")
    valid_file = gzip.open(output_folder + 'valid_labels.txt.gz', 'wt')
    writer_valid = csv.writer(valid_file, delimiter="\t")
    test_file = gzip.open(output_folder + 'test.txt.gz', 'wt')
    writer_test = csv.writer(test_file, delimiter="\t")
    if model_name == "pgpr":
        for k in uid2pids_timestamp_tuple.keys():
            curr = uid2pids_timestamp_tuple[k]
            
            # random.shuffle(curr)
            
            n = len(curr)
            last_idx_train = int(n * train_size)
            pids_train = curr[:last_idx_train]
            for pid, timestamp in pids_train:
                writer_train.writerow([k, pid, 1, timestamp])
            if valid_size != 0:
                last_idx_valid = last_idx_train + int(n * valid_size)
                pids_valid = curr[:last_idx_valid]
                for pid, timestamp in pids_valid:
                    writer_valid.writerow([k, pid, 1, timestamp])
            else:
                last_idx_valid = last_idx_train
            pids_test = curr[last_idx_valid:]
            for pid, timestamp in pids_test:
                writer_test.writerow([k, pid, 1, timestamp])
    train_file.close()
    valid_file.close()
    test_file.close()

def get_time_based_train_test_split(dataset_name, model_name, train_size, valid_size=0):
    input_folder = f'data/{dataset_name}/preprocessed/'

    uid2pids_timestamp_tuple = defaultdict(list)
    with open(input_folder + 'ratings.txt', 'r') as ratings_file: #uid	pid	rating	timestamp
        reader = csv.reader(ratings_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            uid, pid, rating, timestamp = row
            uid2pids_timestamp_tuple[uid].append([pid, int(timestamp)])
    ratings_file.close()

    for uid in uid2pids_timestamp_tuple.keys():
        uid2pids_timestamp_tuple[uid].sort(key=lambda x: x[1])

    train = defaultdict(list)
    valid = defaultdict(list)
    test = defaultdict(list)
    if model_name == "pgpr":
        for uid in uid2pids_timestamp_tuple.keys():
            curr = uid2pids_timestamp_tuple[uid]
            
            # random.shuffle(curr)
            
            n = len(curr)
            last_idx_train = int(n * train_size)
            pids_train = curr[:last_idx_train]
            for pid, timestamp in pids_train:
                train.append([uid, pid, 1, timestamp])
            if valid_size != 0:
                last_idx_valid = last_idx_train + int(n * valid_size)
                pids_valid = curr[:last_idx_valid]
                for pid, timestamp in pids_valid:
                    valid.append([uid, pid, 1, timestamp])
            else:
                last_idx_valid = last_idx_train
            pids_test = curr[last_idx_valid:]
            for pid, timestamp in pids_test:
                test.append([uid, pid, 1, timestamp])
    return train, valid, test

def map_to_PGPR(dataset_name):
    if dataset_name == CELL:
        map_to_PGPR_amazon(dataset_name)
        return
    input_folder = f'data/{dataset_name}/preprocessed/'
    input_folder_kg = f'data/{dataset_name}/preprocessed/'
    output_folder = f'data/{dataset_name}/preprocessed/pgpr/'

    ensure_dir(output_folder)

    relation_id2entity = {}
    relation_id2relation_name = {}
    with open(input_folder_kg + 'r_map.txt', 'r') as relation_file:
        reader = csv.reader(relation_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            relation_id, relation_url = int(row[0]), row[1]
            relation_id2entity[relation_id] = relation_to_entity[dataset_name][relation_url]
            relation_id2relation_name[relation_id] = relation_id2entity[relation_id] + f'_p_{relation_id2entity[relation_id][:2]}'
    relation_file.close()

    entity_type_id2plain_name = defaultdict(dict)
    org_datasetid2movie_title = {}
    if dataset_name == ML1M:
        with open(f'data/{dataset_name}/movies.dat', 'r', encoding="latin-1") as org_movies_file:
            reader = csv.reader(org_movies_file)
            next(reader, None)
            for row in reader:
                row = row[0].split("::")
                org_datasetid2movie_title[row[0]] = row[1]
        org_movies_file.close()
    elif dataset_name == LFM1M:
        with open(f'data/{dataset_name}/tracks.txt', 'r', encoding="latin-1") as org_movies_file:
            reader = csv.reader(org_movies_file)
            for row in reader:
                row = row[0].split("\t")
                org_datasetid2movie_title[row[0]] = row[1]
        org_movies_file.close()

    entity2dataset_id = {}
    with open(input_folder_kg + 'i2kg_map.txt', 'r') as item_to_kg_file:
        reader = csv.reader(item_to_kg_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            dataset_id, entity_id = row[0], row[-1]
            entity2dataset_id[entity_id] = dataset_id

    item_to_kg_file.close()

    dataset_id2new_id = {}
    with open(input_folder + "products.txt", 'r') as item_to_kg_file:
        reader = csv.reader(item_to_kg_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            new_id, dataset_id = row[0], row[1]
            entity_type_id2plain_name["product"][new_id] = org_datasetid2movie_title[dataset_id]
            dataset_id2new_id[dataset_id] = new_id
    item_to_kg_file.close()

    triplets_groupby_entity = defaultdict(set)
    relation_pid_to_entity = {relation_file_name: defaultdict(list) for relation_file_name in relation_id2relation_name.values()}
    with open(input_folder_kg + 'kg_final.txt', 'r') as kg_file:
        reader = csv.reader(kg_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            entity_head, entity_tail, relation = row[0], row[1], row[2]
            triplets_groupby_entity[relation_id2entity[int(relation)]].add(entity_tail)
            dataset_new_id = dataset_id2new_id[entity2dataset_id[entity_head]]
            relation_pid_to_entity[relation_id2relation_name[int(relation)]][dataset_new_id].append(entity_tail)
    kg_file.close()

    entity_to_entity_url = {}
    with open(input_folder_kg + 'e_map.txt', 'r') as entities_file:
        reader = csv.reader(entities_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            entity_id, entity_url = row[0], row[1]
            entity_to_entity_url[entity_id] = entity_url
    entities_file.close()

    entity_id2new_id = defaultdict(dict)
    for entity_name, entity_list in triplets_groupby_entity.items():
        with gzip.open(output_folder + f'{entity_name}.txt.gz', 'wt') as entity_file:
            writer = csv.writer(entity_file, delimiter="\t")
            writer.writerow(['new_id', 'name'])
            for new_id, entity in enumerate(entity_list):
                writer.writerow([new_id, entity])
                if entity in entity_to_entity_url:
                    entity_type_id2plain_name[entity_name][new_id] = entity_to_entity_url[entity]
                else:
                    entity_type_id2plain_name[entity_name][new_id] = new_id
                entity_id2new_id[entity_name][entity] = new_id
        entity_file.close()

    with gzip.open(output_folder + f'mappings.txt.gz', 'wt') as mapping_file:
        writer = csv.writer(mapping_file, delimiter="\t")
        for entity_type, entities in entity_type_id2plain_name.items():
            for entity in entities:
                name = entity_type_id2plain_name[entity_type][entity]
                name = name.split("/")[-1] if type(name) == str else str(name)
                writer.writerow([f"{entity_type}_{entity}", name])
    mapping_file.close()

    for relation_name, items_list in relation_pid_to_entity.items():
        entity_name = relation_name2entity_name[dataset_name][relation_name]
        with gzip.open(output_folder + f'{relation_name}.txt.gz', 'wt') as relation_file:
            writer = csv.writer(relation_file, delimiter="\t")
            #writer.writerow(['new_id', 'name'])
            for i in range(len(dataset_id2new_id.keys())+1):
                entity_list = items_list[str(i)]
                entity_list_mapped = [entity_id2new_id[entity_name][entity_id] for entity_id in entity_list]
                writer.writerow(entity_list_mapped)
        relation_file.close()

    with gzip.open(output_folder + 'products.txt.gz', 'wt') as product_fileo:
        writer = csv.writer(product_fileo, delimiter="\t")
        with open(input_folder + 'products.txt', 'r') as product_file:
            reader = csv.reader(product_file, delimiter="\t")
            for row in reader:
                writer.writerow(row)
        product_file.close()
    product_fileo.close()

    with gzip.open(output_folder + 'users.txt.gz', 'wt') as users_fileo:
        writer = csv.writer(users_fileo, delimiter="\t")
        with open(input_folder + 'users.txt', 'r') as users_file:
            reader = csv.reader(users_file, delimiter="\t")
            for row in reader:
                writer.writerow(row)
        users_file.close()
    users_fileo.close()

def map_to_PGPR_amazon(dataset_name):
    input_folder = f'data/{dataset_name}/preprocessed/'
    input_folder_kg = f'data/{dataset_name}/preprocessed/'
    output_folder = f'data/{dataset_name}/preprocessed/pgpr/'

    ensure_dir(output_folder)

    relation_id2entity = {}
    relation_id2relation_name = {}
    with open(input_folder_kg + 'r_map.txt', 'r') as relation_file:
        reader = csv.reader(relation_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            relation_id, relation_url = int(row[0]), row[1]
            relation_id2entity[relation_id] = relation_to_entity[dataset_name][relation_url]
            if relation_id in [1, 2, 4, 5]:
                relation_id2relation_name[relation_id] = relation_url + f'_p_{relation_id2entity[relation_id][:2]}'
            else:
                relation_id2relation_name[relation_id] = relation_id2entity[relation_id] + f'_p_{relation_id2entity[relation_id][:2]}'
    relation_file.close()

    entity2dataset_id = {}
    with open(input_folder_kg + 'i2kg_map.txt', 'r') as item_to_kg_file:
        reader = csv.reader(item_to_kg_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            dataset_id, entity_id = row[0], row[-1]
            entity2dataset_id[entity_id] = dataset_id
    item_to_kg_file.close()

    dataset_id2new_id = {}
    with open(input_folder + "products.txt", 'r') as item_to_kg_file:
        reader = csv.reader(item_to_kg_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            new_id, dataset_id = row[0], row[1]
            dataset_id2new_id[dataset_id] = new_id
    item_to_kg_file.close()

    triplets_groupby_entity = defaultdict(set)
    relation_pid_to_entity = {relation_file_name: defaultdict(list) for relation_file_name in relation_id2relation_name.values()}
    with open(input_folder_kg + 'kg_final.txt', 'r') as kg_file:
        reader = csv.reader(kg_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            entity_head, entity_tail, relation = row[0], row[1], row[2]
            if relation == "1" or relation == "4":
                triplets_groupby_entity['related_product'].add(entity_tail)
                relation_pid_to_entity[relation_id2relation_name[int(relation)]][entity_head].append(entity_tail)
            elif relation == "1" or relation == "4":
                triplets_groupby_entity['product'].add(entity_tail)
                relation_pid_to_entity[relation_id2relation_name[int(relation)]][entity_head].append(entity_tail)
            else:
                triplets_groupby_entity[relation_id2entity[int(relation)]].add(entity_tail)
                relation_pid_to_entity[relation_id2relation_name[int(relation)]][entity_head].append(entity_tail)
    kg_file.close()

    entity_to_entity_url = {}
    with open(input_folder_kg + 'e_map.txt', 'r') as entities_file:
        reader = csv.reader(entities_file, delimiter="\t")
        next(reader, None)
        for row in reader:
            entity_id, entity_url = row[0], row[1]
            entity_to_entity_url[entity_id] = entity_url
    entities_file.close()

    entity_id2new_id = defaultdict(dict)
    for entity_name, entity_list in triplets_groupby_entity.items():
        if entity_name == "product":
            for new_id, entity in enumerate(set(entity_list)):
                entity_id2new_id[entity_name][entity] = new_id
            continue
        with gzip.open(output_folder + f'{entity_name}.txt.gz', 'wt') as entity_file:
            writer = csv.writer(entity_file, delimiter="\t")
            writer.writerow(['new_id', 'name'])
            for new_id, entity in enumerate(set(entity_list)):
                writer.writerow([new_id, entity])
                entity_id2new_id[entity_name][entity] = new_id
        entity_file.close()

    for relation_name, items_list in relation_pid_to_entity.items():
        entity_name = relation_name2entity_name[dataset_name][relation_name]
        #print(relation_name, entity_name)
        with gzip.open(output_folder + f'{relation_name}.txt.gz', 'wt') as relation_file:
            writer = csv.writer(relation_file, delimiter="\t")
            #writer.writerow(['new_id', 'name'])
            for i in range(len(dataset_id2new_id.keys())+1):
                entity_list = items_list[str(i)]
                entity_list_mapped = [entity_id2new_id[entity_name][entity_id] for entity_id in entity_list]
                writer.writerow(entity_list_mapped)
        relation_file.close()

    with gzip.open(output_folder + 'products.txt.gz', 'wt') as product_fileo:
        writer = csv.writer(product_fileo, delimiter="\t")
        with open(input_folder + 'products.txt', 'r') as product_file:
            reader = csv.reader(product_file, delimiter="\t")
            for row in reader:
                writer.writerow(row)
        product_file.close()
    product_fileo.close()

    with gzip.open(output_folder + 'users.txt.gz', 'wt') as users_fileo:
        writer = csv.writer(users_fileo, delimiter="\t")
        with open(input_folder + 'users.txt', 'r') as users_file:
            reader = csv.reader(users_file, delimiter="\t")
            for row in reader:
                writer.writerow(row)
        users_file.close()
    users_fileo.close()