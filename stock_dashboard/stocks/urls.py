from django.urls import path
from .views import stock_data_view

urlpatterns = [
    path('', stock_data_view, name= 'stock-data')
]