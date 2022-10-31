import json
import time
import requests
from datetime import datetime, timedelta
from airflow.models import DAG
# import instagrapi
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from instagrapi import Client
# from instagrapi import Client
import pandas as pd
# from instagramy import InstagramUser
# from instagram.client import InstagramAPI
cli = Client()

args = {
    'owner': 'Meet',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 30) ,
    'email': ['meetkothari@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=7),
    'schedule_interval': None,
    'provide_context': True
}

def get_user_generaldata(ti,**kwargs):
    extractdata = ['username','full_name','is_private','profile_pic_url','is_verified','media_count','follower_count','following_count','biography','external_url','account_type','is_business','public_email','business_category_name','category_name']
    # get username from API
    variable = kwargs['dag_run'].conf.get('username')
    print(variable)
    # To store Userinformation
    main_data = []

    user_bio_data = cli.user_info_by_username(variable).dict() 
    print(user_bio_data)
    
    for i , j in user_bio_data.items():
        if i  in extractdata:
            main_data.append({"{}".format(i): user_bio_data[i]})
    # Push Data to use in another Dag
    ti.xcom_push(key="main_data_dict",value =main_data )
    ti.xcom_push(key="user_bio_data",value =user_bio_data)

    print("Data is completed")
    print(main_data[0]["username"])

def media_data(ti,**kwargs):

    user_all_v1 = ti.xcom_pull(key="main_data_dict")
    print(user_all_v1)

    user_bio_data_v1 = ti.xcom_pull(key="user_bio_data")
    print(user_bio_data_v1)

    print(user_bio_data_v1["username"])
    if int(user_bio_data_v1["media_count"]) >= 100:
        amount_data_fetch = 100
    else:
        amount_data_fetch = int(user_bio_data_v1["media_count"])
    print(amount_data_fetch,user_bio_data_v1["media_count"])


    image_pk = []
    media_type = []
    reels_url = []
    video_url = []
    image_url = []
    user_medias_v1 = cli.user_medias_v1(int(user_bio_data_v1["pk"]),int(amount_data_fetch))
    for i in user_medias_v1:
        image_pk.append(i.pk)
        media_type.append(i.media_type)
        if  i.media_type == 2:
            if (i.video_url  and i.product_type == "clips"):
                reels_url.append("".join(i.video_url.split("("))),image_url.append(None),video_url.append(None)    
            else:
                video_url.append(None),reels_url.append(None),image_url.append(None)     
        elif i.media_type == 1:
            video_url.append(None),reels_url.append(None)
            if  i.product_type == "feed" : image_url.append("".join(i.thumbnail_url.split("(")))
        elif i.media_type == 8 and i.product_type== "carousel_container":
            reels_url.append(None)
            list_of_list_image = []
            list_of_list_video = []
            for i in i.resources:
                if i.media_type==1:list_of_list_image.append("".join(i.thumbnail_url.split("(")))
                else: list_of_list_video.append("".join(i.video_url.split("(")))
        
            image_url.append(list_of_list_image),video_url.append(list_of_list_video)
    data_medias = {
            "image_pk":image_pk,
            "media_type":media_type,
            "reels_url":reels_url,
            "video_url":video_url,
            "image_url":image_url
        }
    print(len(image_pk)),print(len(video_url)),print(len(media_type)),print(len(reels_url)),print(len(image_url)) 
    ti.xcom_push(key="Medias_data",value =data_medias)   
    print("dag Run Sucess")  


def data_csv(ti,**kwargs):
    data_medias = ti.xcom_pull(key="Medias_data")
    data = data_medias["image_pk"]
    user_bio_info = ti.xcom_pull(key="user_bio_data")
    import pandas as pd
    data_medias_csv = {
        "user_unique_pk": user_bio_info["pk"] ,
        "username":user_bio_info["username"],
        "full_name":user_bio_info["full_name"],
        "is_private":user_bio_info["is_private"],
        "profile_pic_url":user_bio_info["profile_pic_url"],
        "is_verified":user_bio_info["is_verified"],
        "media_count":user_bio_info["media_count"],
        "follower_count":user_bio_info["follower_count"],
        "following_count":user_bio_info["following_count"],
        "biography":user_bio_info["biography"],
        "external_url":user_bio_info["external_url"],
        "account_type":user_bio_info["account_type"],
        "is_business":user_bio_info["is_business"],
        "public_email":user_bio_info["public_email"],
        "business_category_name":user_bio_info["business_category_name"],
        "category_name":user_bio_info["category_name"],
        "image_pk":[data_medias["image_pk"]],
        "media_type":[data_medias["media_type"]],
        "reels_url":[data_medias["reels_url"]],
        "video_url":[data_medias["video_url"]],
        "image_url":[data_medias["image_url"]]
    }
    data_frame_csv = pd.DataFrame(data_medias_csv)
    import os
    #to get the current working directory
    directory = os.getcwd()
    # give permission to dags folder to execute and store file using 777 rule
    data_frame_csv.to_csv(directory+"/data/userdata.csv",mode="a+", index=False, header=(not os.path.exists(directory+"/data/userdata.csv")))
    print("Success")

def dataframe_to_database(ti,**kwargs):

    return "true"


with DAG(
    dag_id='rest-trigger-v1',
    default_args=args,
    catchup=False  # Catchup
) as dag:
    training_model_A = PythonOperator(
    task_id="training_model_A",
    python_callable=get_user_generaldata
    )
    training_model_B = PythonOperator(
    task_id="training_model_B",
    python_callable=media_data
    )
    training_model_C = PythonOperator(
    task_id="training_model_C",
    python_callable=data_csv
    )
training_model_A  >> training_model_B >> training_model_C