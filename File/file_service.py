from boto3.dynamodb.conditions import Attr
from boto3 import resource
from django.conf import settings
from django.http import JsonResponse
import openpyxl
from boto3.dynamodb.types import TypeSerializer
from django.http import HttpResponse
import boto3
import decimal
from boto3.dynamodb.conditions import Key


AWS_ACCESS_KEY_ID = settings.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = settings.AWS_SECRET_ACCESS_KEY
REGION_NAME = settings.REGION_NAME
ENDPOINT_URL = settings.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url='http://localhost:8111',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

MovieTable = resource.Table('Movie')

def create_table_movie():
    print("statement", resource)
    table = resource.create_table(
        TableName='Movie',  # Name of the table
        KeySchema=[
            {
                'AttributeName': 'imdb_title_id',
                'KeyType': 'HASH'  # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'imdb_title_id',  # Name of the attribute
                'AttributeType': 'S'  # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table



# Upload the Excel CSV files to Dyanmo DB
def write_to_DB(imdb_title_id, title, original_title, year, date_published, genre, duration, country, language, director, writer, production_company, actors,
                description, avg_vote, votes, budget, usa_gross_income, worlwide_gross_income, metascore, reviews_from_users, reviews_from_critics):
    response = MovieTable.put_item(
        Item={
                'imdb_title_id': imdb_title_id,
                'title': title,
                'original_title': original_title,
                'year': year,
                'date_published': date_published,
                'genre': genre,
                'duration': duration,
                'country': country,
                'language': language,
                'director': director,
                'writer': writer,
                'production_company': production_company,
                'actors': actors,
                'description': description,
                'avg_vote': avg_vote,
                'votes': votes,
                'budget': budget,
                'usa_gross_income': usa_gross_income,
                'worlwide_gross_income': worlwide_gross_income,
                'metascore': metascore,
                'reviews_from_users': reviews_from_users,
                'reviews_from_critics': reviews_from_critics
        }
    )
    return response


def read_movie_details(imdb_title_id):
    if not isinstance(imdb_title_id, str):
        imdb_title_id = str(imdb_title_id)

    response = MovieTable.get_item(
        key={
            'imdb_title_id': imdb_title_id
        },
        AttributesToGet=[ 'title', 'original_title', 'year', 'date_published', 'genre', 'duration',
                         'country', 'language', 'director', 'writer', 'production_company', 'actors', 'description',
                         'avg_vote', 'votes', 'budget', 'usa_gross_income', 'worlwide_gross_income', 'metascore',
                         'reviews_from_users', 'reviews_from_critics']
    )
    return response


def get_movie_list(request):
    response = MovieTable.scan()
    movies = []
    for item in response['Items']:
        movie = {
            'imdb_title_id': item['imdb_title_id'],
            'title': item['title'],
            'original_title': item['original_title'],
            'year': item['year'],
            'date_published': item['date_published'],
            'genre': item['genre'],
            'duration': item['duration'],
            'country': item['country'],
            'language': item['language'],
            'director': item['director'],
            'writer': item['writer'],
            'production_company': item['production_company'],
            'actors': item['actors'],
            'description': item['description'],
            'avg_vote': decimal.Decimal(item['avg_vote']),
            'votes': int(item['votes']),
            'budget': item['budget'],
            'usa_gross_income': item['usa_gross_income'],
            'worlwide_gross_income': item['worlwide_gross_income'],
            'metascore': item['metascore'],
            'reviews_from_users': item['reviews_from_users'],
            'reviews_from_critics': item['reviews_from_critics']
        }
        movies.append(movie)
    return JsonResponse(movies, safe=False)


def get_movies_by_director(director_name, year1, year2):
    response = MovieTable.scan(
        FilterExpression=Attr('director').eq(director_name),
    )
    movies = []
    for item in response['Items']:
        movie = {
            'imdb_title_id': item['imdb_title_id'],
            'title': item['title'],
            'year': item['year']
        }
        movies.append(movie)
    return JsonResponse(movies, safe=False)

def user_review_response(user_review):
    #print(str(user_review))
    response = MovieTable.scan(
        FilterExpression =Attr('reviews_from_users').gt(str(user_review)) & Attr('language').eq('English'),
    )
    movies = []
    for item in response['Items']:
        movie = {
            'imdb_title_id': item['imdb_title_id'],
            'title': item['title'],
            'year': item['year'],
            'reviews_from_users': item['reviews_from_users']
        }
        movies.append(movie)
    return JsonResponse(movies, safe=False)

def get_highest_budget_titles(country, year):
    response = MovieTable.scan(
        FilterExpression=Attr('year').eq(year) & Attr('country').eq(country),
    )
    movies = []
    for item in response['Items']:
        movie = {
            'imdb_title_id': item['imdb_title_id'],
            'title': item['title'],
            'year': item['year'],
            'budget': item['budget']
        }
        movies.append(movie)
    return JsonResponse(movies, safe=False)
