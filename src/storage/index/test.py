# 读取日志文件并解析每一行
def read_log_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return lines

# 解析每一行日志并提取信息
def parse_log_line(line):
    parts = line.strip().split()
    timestamp = parts[0] + " " + parts[1]
    action = parts[2]
    page_id = int(parts[3])
    return timestamp, action, page_id

# 计算 FetchPgImp, NewPgImp 和 UnpinPgImp 的数量
def analyze_log(log_lines):
    new_counts = {}
    fetch_counts = {}
    unpin_counts = {}

    for line in log_lines:
        timestamp, action, page_id = parse_log_line(line)
        if action == "NewPgImp":
            if page_id in new_counts:
                new_counts[page_id] += 1
            else:
                new_counts[page_id] = 1
        elif action == "FetchPgImp":
            if page_id in fetch_counts:
                fetch_counts[page_id] += 1
            else:
                fetch_counts[page_id] = 1
        elif action == "UnpinPgImp":
            if page_id in unpin_counts:
                unpin_counts[page_id] += 1
            else:
                unpin_counts[page_id] = 1

    return new_counts, fetch_counts, unpin_counts

# 输出每个页面ID的数量
def output_counts(new_counts, fetch_counts, unpin_counts):
    all_page_ids = set(new_counts.keys()).union(fetch_counts.keys()).union(unpin_counts.keys())
    
    for page_id in sorted(all_page_ids):
        new_count = new_counts.get(page_id, 0)
        fetch_count = fetch_counts.get(page_id, 0)
        unpin_count = unpin_counts.get(page_id, 0)
        print(f"Page ID {page_id}: New+Fetch Count = {new_count + fetch_count}, Unpin Count = {unpin_count}")

# 示例文件路径
log_file_path = '/home/sunyifan/study/bustub/src/storage/index/log.txt'

# 读取并分析日志文件
log_lines = read_log_file(log_file_path)
new_counts, fetch_counts, unpin_counts = analyze_log(log_lines)

# 输出结果
output_counts(new_counts, fetch_counts, unpin_counts)
