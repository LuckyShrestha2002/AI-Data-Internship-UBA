try:
    file_name = input("Enter file name: ")

    with open(file_name, 'r') as file:
        content = file.read()
        words = content.split()
        print("Total words:", len(words))

except FileNotFoundError:
    print("Error: File does not exist.")