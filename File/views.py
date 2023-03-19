import csv
from django.http.response import JsonResponse
from django.http import HttpResponse
from rest_framework.parsers import JSONParser
from rest_framework import status
from rest_framework.decorators import api_view
import json

from . import file_service as dynamodb
from .file_service import write_to_DB

@api_view(['GET'])
def home(request):
    return HttpResponse("Hello API's")

@api_view(['GET'])
def create_table(request):
    try:
        dynamodb.create_table_movie()
        return JsonResponse('Table Created', status=status.HTTP_201_CREATED, safe=False)
    except:
        return JsonResponse('Error while creating table', status=status.HTTP_500_INTERNAL_SERVER_ERROR, safe=False)

@api_view(['POST'])
def movie_list(request):
    if request.method == "POST":
        csv_file = request.FILES['file']  # Assuming the file is sent with key 'file'
        reader = csv.DictReader(csv_file.read().decode('utf-8').splitlines())
        start = 0;
        for movie_data in reader:
            if start == 0:
                start = 1
            try:
                print(movie_data['imdb_title_id'])
                response = dynamodb.write_to_DB(movie_data['imdb_title_id'],movie_data['title'],movie_data['original_title'],movie_data['year'],movie_data['date_published'],
                                                movie_data['genre'],movie_data['duration'],movie_data['country'],movie_data['language'],movie_data['director'],movie_data['writer'],movie_data['production_company'],movie_data['actors'],
                                                movie_data['description'],movie_data['avg_vote'],movie_data['votes'],movie_data['budget'],movie_data['usa_gross_income'],movie_data['worlwide_gross_income'],movie_data['metascore'],
                                                movie_data['reviews_from_users'],movie_data['reviews_from_critics'])
                print("running ---- ")
            except:
                return  JsonResponse({'message': 'book not added'}, status= status.HTTP_500_INTERNAL_SERVER_ERROR)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    return JsonResponse({'message': 'Book Added'}, status=status.HTTP_201_CREATED)


@api_view(['GET','PUT','DELETE'])
def movie_detail(request):
    if request.method == 'GET':
        response = dynamodb.get_movie_list(request)
        return response


@api_view(['GET'])
def movie_details_by_Director(request,director, year1, year2):
    if request.method == 'GET':
        director = director
        year1 = int (year1)
        year2 = int (year2)
        response = dynamodb.get_movies_by_director(director, year1, year2)
    return response

@api_view(['GET'])
def user_review_report(request, user_review):
    if request.method == 'GET':
        user_review = user_review
        response = dynamodb.user_review_response(user_review)
    return response

@api_view(['GET'])
def user_highest_budget(request, country, year):
    if request.method == 'GET':
        response = dynamodb.get_highest_budget_titles(country, year)
    return response


