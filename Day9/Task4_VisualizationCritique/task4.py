import matplotlib.pyplot as plt

# -----------------------------
# REDESIGN 1: Pie → Bar Chart
# -----------------------------
def redesign_chart1():
    categories = ["Apple", "Huwaei", "Google", "Nokia", "Sony", "Motorola", "Lenovo", "Realme", "Vivo", "Oppo", "Oneplus", "Xiaomi", "Samsung"]
    values = [20, 10, 2, 2, 1, 2, 3, 4, 5, 6, 2, 7, 20]

    plt.figure(figsize=(8, 5))
    plt.bar(categories, values, color="steelblue")
    plt.xticks(rotation=45, ha='right', fontsize=9)

    plt.title("Redesigned Visualization Piechart to Barchart")
    plt.xlabel("Smartphone Brands", fontsize=12)
    plt.ylabel("Market Shares in Percent", fontsize=12)

    plt.tight_layout()
    plt.savefig("redesign_chart1.png")
    plt.show()


# -----------------------------
# REDESIGN 2: Line Chart Improvement
# -----------------------------
def redesign_chart2():
    years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    sales = [100, 120, 180, 260, 320, 400, 450]

    plt.figure(figsize=(8, 5))

    plt.plot(years, sales, marker="o", linewidth=2)

    plt.title("Improved Trend Visualization of Sales Performance")
    plt.xlabel("Year")
    plt.ylabel("Sales(In Millions of Dollars)")

    plt.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    plt.savefig("redesign_chart2.png")
    plt.show()


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    redesign_chart1()
    redesign_chart2()