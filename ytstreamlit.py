import googleapiclient.discovery
import pandas as pd
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import iso8601
import isodate
import re
import streamlit as st

api_service_name = "youtube"
api_version = "v3"

api_key='AIzaSyBCVqIrgN793za5T7T-zCvHcZW1Gfze9hU'
youtube = googleapiclient.discovery.build(api_service_name,
                                             api_version,
                                             developerKey=api_key)

st.set_page_config(layout='wide')
st.title(":red[Data Harvesting and Warehousing from YOUTUBE]")

with st.sidebar:
     st.title(":orange[Youtube Data Harvesting and Warehousing]")
     st.header(":blue[Overall Processes]")
     st.write("Codings in Python")
     st.write("API Reference")
     st.write("MySQL Data Management")
     st.write("Streamlit Result")

channel_id = st.text_input(":blue[Enter Channel ID]")


def channel_data (channel_id):
     try:
          try:   
               request = youtube.channels().list(
               part="snippet,contentDetails,statistics",
               id= channel_id)
               response = request.execute()

               if 'items' not in response:
                         st.error(f"Invalid Channel id: {channel_id}")
                         st.error("Enter the Valid Channel id")
                         return None
          except HttpError as e:
               st.error('Something Wrong, Check your Internet Connection & Try again!', icon='🚨')
               st.error('An error occurred: %s' % e)
               return None
     except:
               st.error('Limit Reached, Try again later!.')


     data = {       'Channel_Id'    : channel_id,
                    'Channel_name'  : response['items'][0]['snippet']['title'],
                    'Description'   : response['items'][0]['snippet']['description'],
                    'Playlist_id'   : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    'Viewcount'     : response['items'][0]['statistics']['viewCount'],
                    'Subscount'     : response['items'][0]['statistics']['subscriberCount'],
                    'Videocount'    : response['items'][0]['statistics']['videoCount']}
     return data

def get_video_ids(channel_id):

     request1=youtube.channels().list(part='contentDetails',id=channel_id)
     response=request1.execute()

     playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

     video_ids=[]

     next_page_token = None

     while True:
          request2=youtube.playlistItems().list(part='snippet',
                                        maxResults=50,
                                        pageToken=next_page_token,
                                        playlistId=playlist_id)
          response=request2.execute()

          for z in range (len(response['items'])):
               video_ids.append(response['items'][z]['snippet']['resourceId']['videoId'])

          next_page_token=response.get('nextPageToken')
          if next_page_token is None:
               break
     return video_ids


def iso8601_to_seconds(duration):

     pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
     match = pattern.match(duration)
     if match:
               hours = int(match.group(1)) if match.group(1) else 0
               minutes = int(match.group(2)) if match.group(2) else 0
               seconds = int(match.group(3)) if match.group(3) else 0
               total_seconds = hours * 3600 + minutes * 60 + seconds
               return total_seconds
     else:
          return None

#All Video Data.

def overall_video_data(Playlist_Information):

     video_data=[]

     for Video_id in Playlist_Information:
          request3= youtube.videos().list(part='snippet,contentDetails,statistics',id=Video_id)
          response2=request3.execute()

          for i in response2['items']:
               duration = iso8601_to_seconds(i['contentDetails'].get('duration'))

               data = {  "Channel_Id"            : channel_id,
                         "Video_Id"              : i['id'],
                         "Video_Title"           : i['snippet']['title'],
                         "Video_Description"     : i['snippet'].get('description'),
                         "Published_At"          : i['snippet']['publishedAt'],
                         "Total_views"           : i['statistics'].get('viewCount'),
                         "Total_likes"           : i['statistics'].get('likeCount'),
                         "Total_Comments"        : i['statistics'].get('commentCount'),
                         "Favorite_Count"        : i['statistics']['favoriteCount'],
                         "Video_duration"        : duration,
                         "Thumbnail"             : i['snippet']['thumbnails']['default']['url'],
                         "Caption_Status"        : i['contentDetails']['caption']}
               video_data.append(data)
     return video_data

#Comment information:

def overall_comment_data(Playlist_Information):

     comment_data=[]

     try:
          for z in Playlist_Information:
               request4=youtube.commentThreads().list(part='snippet',
                                                       videoId=z,
                                                       maxResults=100)
               response3=request4.execute()
          
               for i in response3['items']:
                    data= { "Comment_Id"         : i['snippet']['topLevelComment']['id'],
                         "Video_Id"              : i['snippet']['videoId'],
                         "Text_Commented"        : i['snippet']['topLevelComment']['snippet']['textDisplay'],
                         "Author_Commented"      : i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         "Comment_Published_at"  : i['snippet']['topLevelComment']['snippet']['publishedAt']}
          comment_data.append(data)
     except:
          pass

     return comment_data

# Streamlit Code

get_data=st.button(':violet[Proceed to Manage Data from the above Channel Id]')

if "Get_state" not in st.session_state:
     st.session_state.Get_state = False

if get_data or st.session_state.Get_state:
     st.session_state.Get_state = True

if get_data:

          Channel_information=channel_data(channel_id)
          Playlist_Information=get_video_ids(channel_id)
          Video_information= overall_video_data(Playlist_Information)
          Comment_information=overall_comment_data(Playlist_Information)

          Channel_df=pd.DataFrame([Channel_information])
          Video_df=pd.DataFrame(Video_information)
          Comment_df=pd.DataFrame(Comment_information)
          st.write(":green[Data has been collected and Stored Successfully!]")

# Migrate Data:

if st.button(':violet[Migrate Data to MySQL]'):

          connection = mysql.connector.connect(
          host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
          port = 4000,
          user = "2ZyEJgr7d9Z3zUr.root",
          password = "Q970rGTYSeCyFYk2")
          mycursor = connection.cursor(buffered=True)

          db_connector = f'mysql+mysqlconnector://2ZyEJgr7d9Z3zUr.root:Q970rGTYSeCyFYk2@gateway01.ap-southeast-1.prod.aws.tidbcloud.com/Youtube'
          db_engine = create_engine(db_connector)

          Channel_information=channel_data(channel_id)
          Playlist_Information=get_video_ids(channel_id)
          Video_information= overall_video_data(Playlist_Information)
          Comment_information=overall_comment_data(Playlist_Information)

          Channel_df=pd.DataFrame([Channel_information])
          Video_df=pd.DataFrame(Video_information)
          Comment_df=pd.DataFrame(Comment_information)

     # Channel_Information Table:

          mycursor.execute('create database if not exists Youtube')
          mycursor.execute('USE Youtube')
          mycursor.execute('''create table if not exists Channel_Information(Channel_Id VARCHAR(255) PRIMARY KEY,
                         Channel_name VARCHAR(100),
                         Description TEXT,
                         Playlist_id TEXT,
                         Viewcount INT(255),
                         Subscount INT(255),
                         Videocount INT(255))''')
          connection.commit()
          Channel_df.to_sql('Channel_Information', con=db_engine, if_exists='append', index=False)

     #Video_Information Table

          mycursor.execute('create database if not exists Youtube')
          mycursor.execute('USE Youtube')
          mycursor.execute('''create table if not exists Video_Information(Channel_Id VARCHAR(255),
                         FOREIGN KEY(Channel_Id) references Channel_Information(Channel_Id),
                         Video_Id VARCHAR(255),
                         Video_Title TEXT,
                         Video_Description TEXT,
                         Published_At TEXT,
                         Total_views INT(255),
                         Total_likes INT(255),
                         Total_Comments INT(255),
                         Favorite_Count INT(255),
                         Video_duration INT(255),
                         Thumbnail TEXT,
                         Caption_Status TEXT)''')
          connection.commit()
          Video_df.to_sql('Video_Information', con=db_engine, if_exists='append', index=False)

     #Comment_Information Table

          mycursor.execute('create database if not exists Youtube')
          mycursor.execute('USE Youtube')
          mycursor.execute('''create table if not exists Comment_Information(Video_id VARCHAR(255),Channel_Id VARCHAR (255),
                         FOREIGN KEY (Channel_Id) references Channel_Information(Channel_Id),
                         Comment_Id VARCHAR(255),
                         Video_Id TEXT,
                         Text_Commented TEXT,
                         Author_Commented TEXT,
                         Comment_Published_at TEXT)''')
          connection.commit()
          Comment_df.to_sql('Comment_Information', con=db_engine, if_exists='append', index=False)

          st.success(":green[Data Migration Complete & Ready to Proceed!]")

Show_table=st.selectbox(":blue[Select the Required information]",("--Select--","Channel Information","Video Information","Comment Information"))

connection = mysql.connector.connect(
          host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
          port = 4000,
          user = "2ZyEJgr7d9Z3zUr.root",
          password = "Q970rGTYSeCyFYk2")
mycursor = connection.cursor(buffered=True)

db_connector = f'mysql+mysqlconnector://2ZyEJgr7d9Z3zUr.root:Q970rGTYSeCyFYk2@gateway01.ap-southeast-1.prod.aws.tidbcloud.com/Youtube'
db_engine = create_engine(db_connector)

if Show_table=="Channel Information":
     mycursor.execute("SELECT Channel_name,Channel_id,Viewcount,Subscount,Videocount from Youtube.Channel_Information")

     Channel_df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(Channel_df)

elif Show_table=="Video Information":
     mycursor.execute("SELECT Video_Title,Video_id,Published_At,Video_duration,Total_views,Total_likes from Youtube.Video_Information")

     Video_df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(Video_df)

elif Show_table=="Comment Information":
     mycursor.execute("SELECT * from Youtube.Comment_Information")

     Comment_df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(Comment_df)
     

# 10 Queries:

query_select=st.selectbox(":blue[Select your Question]",("--Select--",
     "1.What are the names of all the videos and their corresponding channels?",
     "2.Which channels have the most number of videos, and how many videos do they have?",
     "3.What are the top 10 most viewed videos and their respective channels?",
     "4.How many comments were made on each video, and what are their corresponding video names?",
     "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
     "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
     "7.What is the total number of views for each channel, and what are their corresponding channel names?",
     "8.What are the names of all the channels that have published videos in the year 2022?",
     "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
     "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))



#Query 1
mycursor.execute('USE Youtube')

if query_select=="1.What are the names of all the videos and their corresponding channels?":
     mycursor.execute("SELECT Youtube.Video_Information.Video_Title, Channel_Information.Channel_name \
     FROM Youtube.video_Information \
     INNER JOIN Channel_Information ON Video_Information.Channel_Id = Video_Information.Channel_Id ")

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":green[Video Name and Respective Channel Name]")
     st.write(df)

#Query 2
elif query_select=="2.Which channels have the most number of videos, and how many videos do they have?":
     mycursor.execute('select Channel_name,Videocount as Max_No_Of_Videos from Youtube.Channel_Information')

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":blue[Name of the Channel and No of Videos Posted]")
     st.write(df)

#Query 3
elif query_select== "3.What are the top 10 most viewed videos and their respective channels?":
     mycursor.execute("SELECT Channel_Information.Channel_name, Video_Information.Total_views \
                    FROM Youtube.Video_Information \
                    JOIN Channel_Information ON Video_Information.Channel_Id = Channel_Information.Channel_Id  \
                    ORDER BY Video_Information.Total_views DESC LIMIT 10")
     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":green[The Top 10 Most viewed videos and Respective Channel Name]")
     st.write(df)

#Query 4
elif query_select=="4.How many comments were made on each video, and what are their corresponding video names?":
     mycursor.execute("SELECT Video_Title,Total_Comments FROM Youtube.Video_Information ORDER BY Total_Comments DESC")

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":blue[Video Name Along with the No of Comments on Each Video]")
     st.write(df)

#Query 5
elif query_select=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
     mycursor.execute("SELECT Channel_Information.Channel_name, Video_Information.Total_likes \
                    FROM Youtube.Video_Information \
                    JOIN Channel_Information ON Video_Information.Channel_Id = Channel_Information.Channel_Id \
                    WHERE Video_Information.Total_likes = (SELECT max(Total_likes) FROM Video_Information)")
     
     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":green[Videos with Maximum Likes along with the respective Channel Name]")
     st.write(df)

#Query 6
elif query_select=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
     mycursor.execute("SELECT Video_Title,Total_likes FROM Youtube.Video_Information ORDER BY Total_likes DESC")

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":blue[The Total likes along with the corresponding Video Title]")
     st.write(df)

#Query 7
elif query_select== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
     mycursor.execute('SELECT Channel_name,Viewcount FROM Youtube.Channel_information ORDER by Viewcount DESC')

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":green[The Total Views of every Channel]")
     st.write(df)

#Query 8
elif query_select=="8.What are the names of all the channels that have published videos in the year 2022?":
     mycursor.execute("SELECT DISTINCT Channel_information.Channel_name \
                    FROM Channel_information \
                    JOIN Video_information ON Channel_Information.Channel_Id = Video_information.Channel_Id \
                    WHERE YEAR(Video_information.Published_At) = 2022")

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":blue[The Name of Channels which published videos on the Year 2022]")
     st.write(df)

#Query 9
elif query_select== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
     mycursor.execute("SELECT Channel_information.Channel_name, SEC_TO_TIME(AVG(Video_information.Video_duration)) AS Average_duration \
                    FROM Youtube.Video_Information \
                    JOIN Channel_Information ON Video_information.Channel_Id = Channel_Information.Channel_Id \
                    GROUP BY Channel_Information.Channel_name")

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":green[The Average Video Duration of every Channel]")
     st.write(df)

#Query 10
elif query_select== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
     mycursor.execute("SELECT Channel_Information.Channel_name, Video_Information.Video_Title, Video_Information.Total_Comments \
                    FROM Video_Information \
                    JOIN Channel_Information ON Video_information.Channel_Id = Channel_Information.Channel_Id \
                    ORDER by (Total_Comments) DESC LIMIT 5")

     df=pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
     st.write(":blue[Videos with Maximum comments and the corresponding Channel Name]")
     st.write(df)

