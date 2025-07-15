AUTHENTICATED = False

import argparse

def require_auth(func):
    def verify_auth(*args, **kwargs):
        global AUTHENTICATED
        if AUTHENTICATED == True:
            return func(*args, **kwargs)
        else:
            return denied()
    return verify_auth
        

@require_auth
def view_dashboard():
    print("Welcome to the Dashboard..!!")
    return
    
def login():
    global AUTHENTICATED
    AUTHENTICATED = True
    print("Login successful..!!")
    return
    
def logout():
    global AUTHENTICATED
    AUTHENTICATED = False
    print("Authentication error you have been logged out.")
    return
    
def denied():
    print("Access denied..")

view_dashboard()
login()
view_dashboard()
logout()
view_dashboard()