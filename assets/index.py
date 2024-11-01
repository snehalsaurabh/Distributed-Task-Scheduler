def print_pattern(name, rows):
    for i in range(1, rows + 1):
        # Create spaces for alignment
        spaces = " " * (rows - i)
        # Create stars with name
        stars = f"{name} " * i
        print(spaces + stars)

def calculate_fibonacci(n):
    if n <= 0:
        return []
    elif n == 1:
        return [0]
    
    fib = [0, 1]
    while len(fib) < n:
        fib.append(fib[-1] + fib[-2])
    return fib

def create_multiplication_table(size):
    table = []
    for i in range(1, size + 1):
        row = []
        for j in range(1, size + 1):
            row.append(i * j)
        table.append(row)
    return table

# Main execution
if __name__ == "__main__":
    # Print a custom pattern
    name = "Vibhor"
    print("\nCreating a pattern with your name:")
    print_pattern(name, 5)
    
    # Calculate and print Fibonacci numbers
    print("\nCalculating first 10 Fibonacci numbers:")
    fib_numbers = calculate_fibonacci(10)
    for i, num in enumerate(fib_numbers):
        print(f"Fibonacci number {i + 1}: {num}")
    
    # Create and print a multiplication table
    print("\nCreating a 5x5 multiplication table:")
    table = create_multiplication_table(5)
    for row in table:
        formatted_row = " ".join(f"{num:3}" for num in row)
        print(formatted_row)
    
    # Create a list of squares and their roots
    print("\nCalculating squares and approximate square roots:")
    numbers = list(range(1, 11))
    squares = {num: (num ** 2, round(num ** 0.5, 2)) for num in numbers}
    for num, (square, root) in squares.items():
        print(f"Number: {num:2} | Square: {square:3} | Square Root: {root}")
    
    # Print the final message multiple times with increasing numbers
    print("\nFinal messages:")
    for i in range(5):
        print(f"{i + 1}. {name} is {'really ' * i}the best!")