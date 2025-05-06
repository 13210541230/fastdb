#include "fastdb.h"
#include <stdio.h>
#include <string.h>
#include <iostream>

// Include platform-specific high-precision timer headers
#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#include <sys/time.h>
#endif

USE_FASTDB_NAMESPACE

// Helper function to get current time in nanoseconds
inline int64_t getNanoTime() {
#ifdef _WIN32
    LARGE_INTEGER freq, counter;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (int64_t)(counter.QuadPart * 1000000000LL / freq.QuadPart);
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
#endif
}

class Stock { 
  public:
    int1   cMarket;       // '0'深圳 '1' 上海
    char*  szStkCode;     // 股票代码
    char*  szStkName;     // 股票名称
    double dLastPrice;    // 股票价格

    Stock() {
        cMarket = '0';
        szStkCode = NULL;
        szStkName = NULL;
        dLastPrice = 0.0;
    }

    TYPE_DESCRIPTOR((KEY(cMarket, HASHED), 
                     KEY(szStkCode, HASHED), 
                     FIELD(szStkName),
                     FIELD(dLastPrice)));
};

REGISTER(Stock);

// 随机生成股票数据
void generateRandomStock(Stock& stock, int i) {
    static const char* szMarketNames[] = {"万科A", "平安银行", "招商银行", "中国石油", "中国石化", 
                                 "上海汽车", "宁德时代", "贵州茅台", "格力电器", "美的集团"};
    
    char szCode[7];
    snprintf(szCode, sizeof(szCode), "%06d", i);
    
    stock.cMarket = (i % 2) ? '0' : '1';  // 交替上海和深圳
    stock.szStkCode = strdup(szCode);
    stock.szStkName = strdup(szMarketNames[i % 10]);
    stock.dLastPrice = 10.0 + (i % 100) * 0.1;  // 生成10-20元之间的价格
}

int main() {
    dbDatabase db;
    Stock stock;
    int64_t start, end, operationStart, operationEnd;
    double totalTime;

    dbDatabase::OpenParameters params;
    params.databaseName = _T("stockdb2");
    params.accessType = dbDatabase::dbAllAccess;
    params.initSize = 1024*1024*1024;         // 初始文件大小设为1GB
    params.extensionQuantum = 512*1024*1024;  // 每次扩展512MB
    params.initIndexSize = 1000000;          // 初始索引支持100万对象
    params.freeSpaceReuseThreshold = 64*1024*1024; // 空间重用阈值
    
    // 打开数据库
    if (db.open(params)) {
        printf("数据库打开成功\n");
        dbCursor<Stock> updateCursor(dbCursorForUpdate);
        updateCursor.select(); // 选择所有记录
        updateCursor.removeAllSelected(); // 删除所有选中记录
        db.commit(); // 提交事务

        // 开始插入计时
        int64_t totalInsertTime = 0;
        start = getNanoTime();
        
        // 执行插入操作
        for (int i = 0; i < 999999; i++) {
            generateRandomStock(stock, i);
            operationStart = getNanoTime();
            db.insert(stock);
            operationEnd = getNanoTime();
            totalInsertTime += (operationEnd - operationStart);
            
            // 释放内存
            free(stock.szStkCode);
            free(stock.szStkName);
        }
        
        // 提交事务
        db.commit();
        
        // 结束插入计时
        end = getNanoTime();
        double totalInsertTimeSeconds = totalInsertTime / 1000000000.0;
        double totalElapsedSeconds = (end - start) / 1000000000.0;
        
        printf("插入999999条记录总耗时: %.6f 秒\n", totalElapsedSeconds);
        printf("纯插入操作耗时: %.6f 秒\n", totalInsertTimeSeconds);
        printf("平均每条记录插入耗时: %.9f 秒 (%.0f 纳秒)\n", 
               totalInsertTimeSeconds / 999999, 
               (double)totalInsertTime / 999999);
        // 测试更新性能
        
        
        int n = updateCursor.select("szStkCode = '000001'"); // 选择所有记录
        start = getNanoTime();
        for (int i = 0; i < n; i++) {
            updateCursor->dLastPrice = 10.3;
            updateCursor.next();
        }
        updateCursor.update();
        db.commit();
        std::cout << "更新第一条记录耗时: " << (getNanoTime() - start) / 1000000000.0 << " 秒\n";
        // n = updateCursor.select("szStkCode = '000100'");
        // for (int i = 0; i < n; i++) {
        //     updateCursor->dLastPrice = 10.2;
        //     updateCursor.next();
        // }
        
        n = updateCursor.select("szStkCode = '990000'");
        start = getNanoTime();
        for (int i = 0; i < n; i++) {
            updateCursor->dLastPrice = 10.3;
            updateCursor.next();
        }
        updateCursor.update();
        db.commit(); // 提交事务
        std::cout << "更新最后一条记录耗时: " << (getNanoTime() - start) / 1000000000.0 << " 秒\n";
        
        // 测试查询性能
        start = getNanoTime();
        
        dbCursor<Stock> cursor;
        int nRecords = cursor.select();
        
        end = getNanoTime();
        totalTime = (end - start) / 1000000000.0;
        printf("查询%d条记录耗时: %.6f 秒 (%.0f 纳秒)\n", 
               nRecords, totalTime, (double)(end - start));
        
        // 显示部分数据
        printf("\n数据库内容示例:\n");
        printf("市场\t代码\t名称\t\t价格\n");
        
        int count = 0;
        do {
            printf("%c\t%s\t%s\t\t%.2f\n", 
                   cursor->cMarket, 
                   cursor->szStkCode, 
                   cursor->szStkName, 
                   cursor->dLastPrice);
            count++;
        } while (cursor.next() && count < 10);  // 只显示前10条

        // 测试删除性能
        start = getNanoTime();
        dbCursor<Stock> deleteCursor(dbCursorForUpdate);
        deleteCursor.select(); // 选择所有记录
        deleteCursor.removeAllSelected(); // 删除所有选中记录
        end = getNanoTime();
        totalTime = (end - start) / 1000000000.0;
        printf("删除999999条记录耗时: %.6f 秒 (%.0f 纳秒)\n", 
               totalTime, (double)(end - start));
        db.commit(); // 提交事务
        
        
        
        // 关闭数据库
        db.close();
        printf("数据库已关闭\n");
        return 0;
    } else {
        printf("无法打开数据库\n");
        return 1;
    }
}