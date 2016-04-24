from django.conf.urls import url
from django.contrib import admin
admin.autodiscover()
from . import views


urlpatterns = [
    url(r'^processmongodata/$', 'pinutcloud.views.processmongodata'),
    url(r'^uploadjsonfiles/$', 'pinutcloud.views.uploadjsonfiles'),
    url(r'^admin/', admin.site.urls),
]

