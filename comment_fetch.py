# importing the module
import praw
import pandas as pd
from time import time
from tqdm import tqdm

# initialize with appropriate values
client_id = ""
client_secret = ""
username = ""
password = ""
user_agent = ""
  
# creating an authorized reddit instance
reddit = praw.Reddit(client_id = '', 
                     client_secret = '', 
                     username = '', 
                     password = '',
                     user_agent = 'redditdev scraper by commentid') 


print(reddit.user.me())
df_ruddit = pd.read_csv("/Users/bhuvanaka/2021/reddit_comment/Ruddit/Dataset/Ruddit.csv")
print(df_ruddit.head())
print(df_ruddit.shape)


# we need a list of post_id to extract comments
posts = df_ruddit.post_id.unique()
# number of unique post_id
len(posts)

# create a dictionary with POST_ID as key and an array of COMMENT_IDs as values
# an optimized method, utilizes pandas groupby groups attribute
r = df_ruddit.groupby('post_id')[['post_id', 'comment_id']]
pairs = dict()
for j in r.groups:
    pairs[j] = df_ruddit['comment_id'].iloc[r.groups[j]].to_numpy()

# do a random check
# what comment_ids are there for a given post_id?
print(pairs)
print(pairs['3vdy9k'])


# collect post ids which lead to errors, like forbidden 403
issue_posts = []
# iterate over all post ids
for p in tqdm(posts[:10], desc='overall progress'): 
    # process 10 posts for demo
    now = time()
    try:
        # create a submission to Reddit API
        submission = reddit.submission(id=p)
        # read the URL
        URL = submission.url
        # flatten the comment tree
        submission.comments.replace_more(limit=None)
    except Exception as e:
        # if there is an error making submission
        issue_posts.append((p, e))
        continue
    delta = int(time()-now)
    # let's know the time taken for each submission
    desc = str(p)+' '+str(delta)+' sec'
    # iterate over actual comment ids 
    for c in tqdm(submission.comments.list(), desc=desc):
        # iff our data contains that id
        if c in pairs[p]:
            # locate in our data
            index = df_ruddit[df_ruddit['comment_id']== str(c)].index
            # replace our data
            df_ruddit.loc[index,['txt','url']] = [c.body,URL+'/'+str(c)+'/']




# drop rows where we don't have texts
# reorder columns for elegance
df_ruddit_1 = df_ruddit.dropna(axis=0,inplace=False)\
[['post_id','comment_id','txt','url','offensiveness_score']]
df_ruddit_1.head()

# if there are problematic post_ids, 
# publish them into a CSV file
if len(issue_posts):
    print(len(issue_posts))
    issue_1 = pd.DataFrame(data=issue_posts, columns=['post_id', 'error_msg'])
    issue_1.to_csv('post_with_issue_1.csv', index=False)
    print(issue_1.head())

# publish our extracted text data
df_ruddit_1.to_csv('ruddit_with_text_1.csv',index=False) 


# # the ID of the comment
# comment_ids = df_ruddit["comment_id"].values.tolist()
# comments = []
# # instantiating the Comment class
# for comment_id in comment_ids[0:10]:
#     comment = reddit.comment(comment_id)
#     # fetching the body of the comment
#     body = comment.body
#     comments.append(body)
    


  
# # printing the body of the comment
# for comment in comments:
#     print("**************")
#     print("The body of the comment is : \n\n" + comment)