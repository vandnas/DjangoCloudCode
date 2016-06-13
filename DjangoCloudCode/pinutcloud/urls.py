from django.conf.urls import url, patterns, include
from django.contrib import admin
admin.autodiscover()
from . import views
#from django.conf import settings
#from django.conf.urls.static import static

urlpatterns = [
    url(r'^processUserIntroMongoData/$', 'pinutcloud.views.processUserIntroMongoData'),
    url(r'^getTotalDownloads/$', 'pinutcloud.views.getTotalDownloads'),
    url(r'^getUserIntroContent/$', 'pinutcloud.views.getUserIntroContent'),
#=============================================================================================
    url(r'^processmongodata/$', 'pinutcloud.views.processUserInfoMongoData'),
    url(r'^popularMovieList/$', 'pinutcloud.views.popularMovieList'),
    url(r'^userDistTimeSlot/$', 'pinutcloud.views.userDistTimeSlot'),
    url(r'^totalUsersConnected/$', 'pinutcloud.views.totalUsersConnected'),
    url(r'^totalPinutDevices/$', 'pinutcloud.views.totalPinutDevices'),
#=============================================================================================
    url(r'^processFeedbackMongoData/$', 'pinutcloud.views.processFeedbackMongoData'),
    url(r'^getTotalFeedback/$', 'pinutcloud.views.getTotalFeedback'),
    url(r'^getAverageFeedback/$', 'pinutcloud.views.getAverageFeedback'),
    url(r'^getFeedbackContent/$', 'pinutcloud.views.getFeedbackContent'),
#=============================================================================================
    url(r'^dashboard/$', 'pinutcloud.views.validatelogin'),
    url(r'^dashboard/login.html/$', 'pinutcloud.views.renderloginpage'),
    url(r'^dashboard/index.html/$', 'pinutcloud.views.renderindexpage'),
    url(r'^dashboard/calendar.html/$', 'pinutcloud.views.rendercalendarpage'),
    url(r'^uploadjsonfiles/$', 'pinutcloud.views.uploadjsonfiles'),
    url(r'^admin/', admin.site.urls),
]

