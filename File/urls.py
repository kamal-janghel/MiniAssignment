from django.urls import path
from . import views

urlpatterns = [
    path('home', views.home),
    path('movies/upload', views.movie_list),
    path('movies/', views.create_table),
    path('movies/read', views.movie_detail),

    #usecase1
    path('movies/<str:director>/<str:year1>/<str:year2>', views.movie_details_by_Director),
    #usecase2
    path('movies/<str:user_review>', views.user_review_report),
    #usecase3
    path('movies/<str:country>/<str:year>', views.user_highest_budget),


]