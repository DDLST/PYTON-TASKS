import pandas as pd
import random
from concurrent.futures import ProcessPoolExecutor as Pool
#Надеюсь я правильно сделал генерацию файлов + паралельную обработку, если что скажите пожалуйста!
def make_files():
    letters = ['A', 'B', 'C', 'D']
    for i in range(5):
        data = []
        for _ in range(100):
            letter = random.choice(letters)
            value = random.uniform(0, 100)
            data.append([letter, value])
        df = pd.DataFrame(data, columns=["Категория", "Значение"])
        df.to_csv(f"file_{i+1}.csv", index=False)
        print("Сделан файл csv под номеромм, можно глянуть папку с файлом:",i+1)

def work_file(fname):
    df = pd.read_csv(fname)
    return df.groupby("Категория")["Значение"].agg(["median", "std"]).reset_index()

def main():
    make_files()

    files = [f"file_{i+1}.csv" for i in range(5)]

    with Pool() as pool:
        results = list(pool.map(work_file, files))

    all_data = pd.concat(results)

    stats = all_data.groupby("Категория").agg({
        "median": "median",
        "std": "std"
    }).reset_index()

    print("Медиана и отклонение по категориям:")
    for _, row in stats.iterrows():
        print(f"{row['Категория']}, {row['median']:.2f}, {row['std']:.2f}")

    stats2 = all_data.groupby("Категория")["median"].agg(["median", "std"]).reset_index()

    print("\nМедиана медиан и отклонение медиан:")
    for _, row in stats2.iterrows():
        print(f"{row['Категория']}, {row['median']:.2f}, {row['std']:.2f}")

if __name__ == "__main__":
    main()
