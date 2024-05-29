#include <ctime>    // 包含时间相关的头文件
#include <fstream>  // 包含文件流的头文件
#include <iostream>
#include <string>

void logToFile(const std::string &message) {
  // 打开或创建日志文件，追加写入方式
  std::ofstream logfile("/home/sunyifan/study/bustub/src/storage/index/log.txt", std::ios::app);
  if (!logfile.is_open()) {
    std::cerr << "Error: Cannot open log file." << std::endl;
    return;
  }

  // 获取当前时间
  std::time_t t = std::time(nullptr);
  std::tm *now = std::localtime(&t);

  // 写入日志信息
  logfile << "[" << (now->tm_year + 1900) << "-" << (now->tm_mon + 1) << "-" << now->tm_mday << " " << now->tm_hour
          << ":" << now->tm_min << ":" << now->tm_sec << "] " << message << std::endl;

  // 关闭日志文件
  logfile.close();
}
