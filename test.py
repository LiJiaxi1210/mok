import csv
import random
import subprocess
from progress.bar import Bar

def get_n_lines(filename):
    result = subprocess.run(['wc', '-l', filename], stdout=subprocess.PIPE)
    return int(result.stdout.split()[0])

def count_ttl_occurrences_and_generate_ttl(filename, max_rows = 0):
    occurrences = {}
    setkey_ttl = {}

    with open(filename, 'r') as file:
        bar = Bar('count_ttl_occurrences_and_generate_ttl', max=100)
        reader = csv.DictReader(file)
        processed_rows = 0
        for row in reader:
            if processed_rows >= max_rows:
                break
            processed_rows += 1

            if row['op'] == 'SET':
                count_value = row['ttl']
                if count_value in occurrences:
                    occurrences[count_value] += 1
                else:
                    occurrences[count_value] = 1
                
                setkey_ttl[row['key']] = count_value

            if processed_rows % update_frequency == 0:
                bar.next()
        bar.finish()

    return occurrences, setkey_ttl

def count_occurrences(filename, target_column, target_value, count_column, max_rows = 0):
    occurrences = {}

    with open(filename, 'r') as file:
        bar = Bar('count_occurrences', max=100)
        reader = csv.DictReader(file)
        processed_rows = 0
        for row in reader:
            if processed_rows >= max_rows:
                break
            processed_rows += 1

            if row[target_column] == target_value:
                count_value = row[count_column]
                if count_value in occurrences:
                    occurrences[count_value] += 1
                else:
                    occurrences[count_value] = 1
            if processed_rows % update_frequency == 0:
                bar.next()
        bar.finish()

    return occurrences

def generate_distribution(distribution):
    total_count = sum(distribution.values())
    probabilities = [count / total_count for count in distribution.values()]
    values = list(distribution.keys())
    return values, probabilities

def generate_k_random_value(values, probabilities, k):
    return random.choices(values, probabilities, k=k)

def generate_ttl(filename, max_rows = 0):
    setkey_ttl = {}

    with open(filename, 'r') as file:
        bar = Bar('generate_ttl', max=100)
        reader = csv.DictReader(file)
        processed_rows = 0
        for row in reader:
            if processed_rows >= max_rows:
                break
            processed_rows += 1
            if row['op'] == 'SET':
                setkey_ttl[row['key']] = row['ttl']
            if processed_rows % update_frequency == 0:
                bar.next()
        bar.finish()

    return setkey_ttl

def main():
# 统计出现次数
    # occurrences = count_occurrences(filename, target_column, target_value, count_column, max_rows)
    # random_value = generate_random_value(occurrences)
    # print(f'Generated random value: {random_value}')
    # ttl = generate_ttl(filename, max_rows)

    occurrences, ttl = count_ttl_occurrences_and_generate_ttl(filename, max_rows)

    v, p = generate_distribution(occurrences)

    random_ttls = generate_k_random_value(v, p, k)

    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        with open(outputfilename, 'w', newline='') as outputfile:
            writer = csv.DictWriter(outputfile,fieldnames=['op_time','key','key_size','op','op_count','size','cache_hits','ttl','usecase','sub_usecase'])
            writer.writeheader()
            processed_rows = 0
            j = 0
            bar = Bar('write file', max=100)
            for row in reader:
                
                if processed_rows >= max_rows:
                    break
                processed_rows += 1

                if row['ttl'] == '0':
                    if row['key'] in ttl:
                        row['ttl'] = ttl[row['key']]
                    else:
                        if j >= k:
                            random_ttls = generate_k_random_value(v, p, k)
                            j = 0
                        row['ttl'] = random_ttls[j]
                        j += 1
                        ttl[row['key']] = row['ttl']
                writer.writerow(row)
                if processed_rows % update_frequency == 0:
                    bar.next()
            bar.finish()
                

if __name__ == '__main__':
    filename = '/home/ms-admin/workspace1/lijiaxi/CacheLib/opt/cachelib/kvcache_202401/kvcache_traces_2.csv'  # CSV 文件路径
    outputfilename = 'modified_2.csv'
    # target_column = 'op'  # 目标列名
    # target_value = 'SET'  # 目标值
    # count_column = 'ttl'  # 统计列名

    # 统计trace最大行数，没有上限设为0
    max_rows = 0

    if max_rows == 0:
        max_rows = get_n_lines(filename)

    update_frequency = max_rows // 100
    # 独立随机采样次数
    k = 100000
    main()

