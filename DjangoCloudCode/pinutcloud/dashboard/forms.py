from django import forms

class NameForm(forms.Form):
    print "In form.py"
    email = forms.EmailField(label='E-mail', max_length=100)
    print "email",email
    password = forms.CharField(label='Password', max_length=50)
    print "password",password
