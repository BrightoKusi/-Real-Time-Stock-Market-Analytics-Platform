from django.urls import path
from .views import stock_data_view
from . import api_views

urlpatterns = [
    path('', stock_data_view, name= 'stock-data'),
    path('api/stocks', api_views.stock_data_api, name='stock_data_api'),
]