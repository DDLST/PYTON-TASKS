import pandas as pd
import random
from concurrent.futures import ProcessPoolExecutor as Pool

def create_files():
    categories = ['A', 'B', 'C', 'D']
    
    for N in range(5):
        data = []
        for _ in range(100):
            category = random.choice(categories)  
            value = random.uniform(0, 100)        
            data.append([category, value])
        
        df = pd.DataFrame(data, columns=["Категория", "Значение"])
        df.to_csv(f"file_{N+1}.csv", index=False)

def process_file(filename):
    df = pd.read_csv(filename)
    result = df.groupby("Категория")["Значение"].agg(["median", "std"]).reset_index()
    return result

def main():
    create_files()
    files = [f"file_{i+1}.csv" for i in range(5)]
    
    with Pool() as executor:
        all_results = list(executor.map(process_file, files))
    
    K = pd.concat(all_results, ignore_index=True)

    simple_stats = K.groupby("Категория").agg({
        "median": "median",
        "std": "std"
    }).reset_index()
    
    print("=== Медиана и среднее отклонение по категориям ===")
    for _, row in simple_stats.iterrows():
        category = row['Категория']
        median_val = row['median']
        std_val = row['std']
        print(f"Категория {category}: медиана = {median_val:.2f}, среднее отклонение = {std_val:.2f}")
    
    final_stats = K.groupby("Категория")["median"].agg(["median", "std"]).reset_index()
    
    print("\n=== Медиана медиан и отклонение медиан ===")
    for _, row in final_stats.iterrows():
        category = row['Категория']
        median_of_medians = row['median']
        std_of_medians = row['std']
        print(f"Категория {category}: медиана медиан = {median_of_medians:.2f}, отклонение медиан = {std_of_medians:.2f}")

if __name__ == "__main__":
    main()
