import requests

url = "https://jsonplaceholder.typicode.com/users"

response = requests.get(url)

if response.status_code == 200:
    users = response.json()

    for user in users:
        name = user["name"]
        email = user["email"]
        city = user["address"]["city"]

        print(f"Name: {name}, Email: {email}, City: {city}")
else:
    print("Failed to fetch data")