import pandas as pd
from pmaw import PushshiftAPI
import praw
import json
import numpy as np

def save_reddit_config(config_dict, filename="redditConfig.txt"):
    with open(filename, "w") as f:
        json.dump(config_dict, f)

def load_reddit_config(filename="redditConfig.txt"):
    with open(filename, "r") as f:
        config = json.load(f)
    return config



def pmaw_pull_subreddit(sub, limit, outfile_root, reddit=None):
    if reddit:
        api = PushshiftAPI(praw=reddit)
        submissions = api.search_submissions(subreddit=sub, limit=limit)
    else:
        api = PushshiftAPI()
        submissions = api.search_submissions(subreddit=sub, limit=limit, mem_safe=True, safe_exit=True, cache_dir="cache")
    sub_df = pd.DataFrame(submissions)
    sub_df.to_csv(f"{outfile_root}.csv", header=True, index=False, columns=list(sub_df.axes[1]))
    print(f"dataframe shape: {sub_df.shape}")
    print(f"WROTE to file: '{outfile_root}.csv'")


def transform_pmaw_pull(file, outfile_root):
    df = pd.read_csv(file)
    df_text = df[["title", "selftext"]]
    df_filtered = df_text[df_text.selftext != "[removed]"]
    df_filtered.to_csv(f"{outfile_root}.txt", sep=' ', index=False, header=False)
    print(f"removed rows with '[removed]' selftext; original shape: {df.shape}, new shape: {df_filtered.shape}")
    print(f"WROTE to file: '{outfile_root}.txt'")


# class PMAWRedditPull:
#     def __init__(self, prawEnrichment=None):
#         self.prawEnrichment = prawEnrichment
#         self.api = self.init_pushshift_conn(self.prawEnrichment)
#         self.ids_list = np.array([])

#     def init_pushshift_conn(self, prawEnrichment):
#         if prawEnrichment:
#             return PushshiftAPI(praw=prawEnrichment)
#         return PushshiftAPI()

#     def filter_unique(self, item):
#         return item["id"] not in self.ids_list

#     def pull_subreddit(self, sub, limit, filter_fn=None):
#         if filter_fn:
#             return self.api.search_submissions(subreddit=sub, limit=limit, filter_fn=filter_fn)
#         return self.api.search_submissions(subreddit=sub, limit=limit)

#     def write_submissions_to_csv(self, submissions, outfile):
#         df_sub = pd.DataFrame(submissions)
#         df_sub.to_csv(outfile, header=True, index=False, columns=list(df_sub.axes[1]))
#         print(f"wrote dataframe of shape '{df_sub.shape}' to file: '{outfile}'")
#         return df_sub.id.to_numpy()
    
    
#     def pull_sub_chunks(self, chunk_size, sub, limit, outfile_root, filter_fn=None):
        
#         frag_index = 0
#         index = 0
        
#         while index < limit:
#             done = False
#             while not done:
#                 try:
#                     submissions = self.pull_subreddit(sub, chunk_size, filter_fn)
#                     if len(submissions) > 0:
#                         done = True
#                 except Exception as e:
#                     print(f"error on frag_index {frag_index}, index {index}: {e}")
            
#             new_ids = self.write_submissions_to_csv(submissions, f"{outfile_root}_frag{frag_index}.csv")
#             self.ids_list = np.concatenate((self.ids_list, new_ids))
#             print(f"total number of submission ids acquired: {self.ids_list.shape[0]}")
#             print(f"frag_index {frag_index}, index: {index}, done with {round((index+chunk_size)/limit, 4) * 100}%\n--------------")

#             frag_index += 1
#             index += chunk_size
        
#         print("DONE")

if __name__ == "__main__":

    sub_default = "jokes"
    sub = input(f"subreddit to pull from [press enter to use default '{sub_default}']: ")
    if sub == "":
        sub = sub_default
    

    limit_default = 10000
    limit = input(f"number of submissions to pull [press enter to use default {limit_default}]: ")
    if limit == "":
        limit = limit_default
    else:
        limit = int(limit)
    

    # chunk_size_default = 1000
    # chunk_size = input(f"size of fragment chunks [press enter to use default {chunk_size_default}]: ")
    # if chunk_size == "":
    #     chunk_size = chunk_size_default
    # else:
    #     chunk_size = int(chunk_size)
    

    outfile_root_default = f"data/{sub}_{limit}"
    outfile_root = input(f"root of CSV filename to write to [press enter to use default: '{outfile_root_default}[.csv]']: ")
    if outfile_root == "":
        outfile_root = outfile_root_default
    

    # prawEnrichment = None
    # usePrawEnrichment = input("use praw enrichment? [y/n]: ")
    # if usePrawEnrichment.lower() == "y":
    #     config = load_reddit_config()
    #     prawEnrichment = praw.Reddit(**config)
    
    # prp = PMAWRedditPull(prawEnrichment)
    # prp.pull_sub_chunks(chunk_size=chunk_size, sub=sub, limit=limit, outfile_root=outfile_root, filter_fn=prp.filter_unique)


    
    pmaw_pull_subreddit(sub=sub, limit=limit, outfile_root=outfile_root, reddit=None)
    # else:
    #     pmaw_pull_subreddit(sub=sub, limit=limit, outfile=outfile)