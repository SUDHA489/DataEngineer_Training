authenticated=False


def logout():
    global authenticated
    authenticated=False
    print("you logged out.")

def login():
    global authenticated
    authenticated=True
    print("you logged in.")

def require_auth(func):
    def wrapper():
        if authenticated:
            func()
        else:
            print("Access denied. Try again")
    return wrapper


@require_auth
def view_dashboard():
    print("Welcome to your dashboard!")


view_dashboard()
login()
view_dashboard()
logout()
view_dashboard()