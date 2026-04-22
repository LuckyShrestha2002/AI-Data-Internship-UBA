def find_largest(numbers):
    largest = numbers[0]   # assume first number is largest

    for num in numbers:
        if num > largest:
            largest = num

    return largest


# Example usage
nums = [10, 45, 23, 67, 89, 2]
result = find_largest(nums)
print("Largest number:", result)