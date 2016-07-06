from django.conf.urls import url, patterns, include
from django.contrib import admin
admin.autodiscover()
from . import views

urlpatterns = [
    url(r'^getTotalDownloads/$', 'pinutcloud.views.getTotalDownloads'),
    url(r'^getUserIntroContent/$', 'pinutcloud.views.getUserIntroContent'),
#=============================================================================================
    url(r'^processUserInfoMongoData/$', 'pinutcloud.views.processUserInfoMongoData'),
#=============================================================================================
    url(r'^getTotalFeedback/$', 'pinutcloud.views.getTotalFeedback'),
    url(r'^getFeedbackRatings/$', 'pinutcloud.views.getFeedbackRatings'),
    url(r'^getFeedbackContent/$', 'pinutcloud.views.getFeedbackContent'),
#=============================================================================================
    url(r'^dashboard/$', 'pinutcloud.views.validatelogin'),
    url(r'^uploadjsonfiles/$', 'pinutcloud.views.uploadjsonfiles'),
    url(r'^admin/', admin.site.urls),
]

