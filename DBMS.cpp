#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <fstream>   // 파일 입출력 헤더 추가
#include <iomanip>   // setw와 setfill을 사용하기 위한 헤더 추가
#include <openssl/evp.h> // OpenSSL EVP 헤더
#define AES_BLOCK_SIZE 16


using namespace std;

// AES 암호화에 사용할 키와 IV (초기화 벡터) 설정
const string AES_KEY = "0123456789abcdef";  // 16 bytes (128 bits)
const string AES_IV = "abcdef0123456789";   // 16 bytes (128 bits)

// AES 암호화 함수
string aesEncrypt(const string& plaintext) {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    EVP_EncryptInit_ex(ctx, EVP_aes_128_cbc(), nullptr, (unsigned char*)AES_KEY.c_str(), (unsigned char*)AES_IV.c_str());

    vector<unsigned char> ciphertext(plaintext.size() + AES_BLOCK_SIZE);
    int len;
    EVP_EncryptUpdate(ctx, ciphertext.data(), &len, (unsigned char*)plaintext.c_str(), plaintext.size());

    int ciphertext_len = len;
    EVP_EncryptFinal_ex(ctx, ciphertext.data() + len, &len);
    ciphertext_len += len;
    EVP_CIPHER_CTX_free(ctx);

    return string(ciphertext.begin(), ciphertext.begin() + ciphertext_len);
}

// AES 복호화 함수
string aesDecrypt(const string& ciphertext) {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), nullptr, (unsigned char*)AES_KEY.c_str(), (unsigned char*)AES_IV.c_str());

    vector<unsigned char> plaintext(ciphertext.size());
    int len;
    EVP_DecryptUpdate(ctx, plaintext.data(), &len, (unsigned char*)ciphertext.c_str(), ciphertext.size());

    int plaintext_len = len;
    EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &len);
    plaintext_len += len;
    EVP_CIPHER_CTX_free(ctx);

    return string(plaintext.begin(), plaintext.begin() + plaintext_len);
}

// 구조체를 사용하여 테이블 스키마 정의
struct TableSchema {
    string tableName;
    vector<string> columns;
};

// 테이블 데이터를 메모리에 저장할 구조체
struct TableData {
    TableSchema schema;
    vector<vector<string>> rows;
};

// 데이터베이스를 메모리에 저장할 구조체
struct Database {
    string dbName;
    unordered_map<string, TableData> tables; // 테이블 이름과 테이블 데이터를 저장
};

// 전역 데이터베이스 저장소
unordered_map<string, Database> databases;
string currentDatabase = "";

// CREATE DATABASE 쿼리를 처리하는 함수
void createDatabase(const string& query) {
    istringstream ss(query);
    string token;
    ss >> token; // 'CREATE'
    ss >> token; // 'DATABASE'

    string dbName;
    ss >> dbName; // 데이터베이스 이름

    if (databases.find(dbName) != databases.end()) {
        cerr << "Database " << dbName << " already exists.\n";
        return;
    }

    Database db;
    db.dbName = dbName;
    databases[dbName] = db;

    cout << "Database " << dbName << " created successfully.\n";
}

// USE DATABASE 쿼리를 처리하는 함수
void useDatabase(const string& query) {
    istringstream ss(query);
    string token;
    ss >> token; // 'USE'

    string dbName;
    ss >> dbName; // 데이터베이스 이름

    if (databases.find(dbName) == databases.end()) {
        cerr << "Database " << dbName << " does not exist.\n";
        return;
    }

    currentDatabase = dbName;
    cout << "Using database " << dbName << ".\n";
}

// CREATE TABLE 쿼리를 처리하는 함수
void createTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "No database selected. Please create or select a database first.\n";
        return;
    }

    istringstream ss(query);
    string token;
    ss >> token; // 'CREATE'
    ss >> token; // 'TABLE'

    TableSchema schema;
    ss >> schema.tableName; // 테이블 이름

    string columnDef;
    while (ss >> columnDef) {
        schema.columns.push_back(columnDef);
    }

    // 테이블을 데이터베이스에 추가
    TableData table;
    table.schema = schema;
    databases[currentDatabase].tables[schema.tableName] = table;

    cout << "Table " << schema.tableName << " created successfully in database " << currentDatabase << ".\n";
}

// INSERT INTO 쿼리를 처리하는 함수
void insertIntoTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "No database selected. Please create or select a database first.\n";
        return;
    }

    istringstream ss(query);
    string token;
    ss >> token; // 'INSERT'
    ss >> token; // 'INTO'

    string tableName;
    ss >> tableName; // 테이블 이름

    // 세미콜론 제거
    tableName.erase(remove(tableName.begin(), tableName.end(), ';'), tableName.end());

    auto it = databases[currentDatabase].tables.find(tableName);
    if (it == databases[currentDatabase].tables.end()) {
        cerr << "Table " << tableName << " does not exist in database " << currentDatabase << ".\n";
        return;
    }

    TableData& table = it->second;
    vector<string> row;
    string value;

    while (ss >> value) {
        row.push_back(value);
    }

    table.rows.push_back(row);
    cout << "Data inserted into " << tableName << " successfully in database " << currentDatabase << ".\n";
}

// SELECT 쿼리를 처리하는 함수
void selectFromTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "No database selected. Please create or select a database first.\n";
        return;
    }

    istringstream ss(query);
    string token;
    ss >> token; // 'SELECT'
    string columns;
    ss >> columns; // 컬럼명 또는 '*'

    ss >> token; // 'FROM'

    string tableName;
    ss >> tableName; // 테이블 이름

    // 세미콜론 제거
    tableName.erase(remove(tableName.begin(), tableName.end(), ';'), tableName.end());

    auto it = databases[currentDatabase].tables.find(tableName);
    if (it == databases[currentDatabase].tables.end()) {
        cerr << "Table " << tableName << " does not exist in database " << currentDatabase << ".\n";
        return;
    }

    TableData& table = it->second;

    // 모든 행 출력
    for (const auto& column : table.schema.columns) {
        cout << column << "\t";
    }
    cout << "\n";

    for (const auto& row : table.rows) {
        for (const auto& value : row) {
            cout << value << "\t";
        }
        cout << "\n";
    }
}

// COMMIT 쿼리를 처리하고 데이터를 AES로 암호화하여 파일에 저장하는 함수
void commitDatabase() {
    if (currentDatabase.empty()) {
        cerr << "No database selected. Please create or select a database first.\n";
        return;
    }

    string filename = currentDatabase + ".mydb";
    ofstream file(filename, ios::binary); // 바이너리 모드로 파일 열기

    if (!file.is_open()) {
        cerr << "Error opening file for writing: " << filename << "\n";
        return;
    }

    Database& db = databases[currentDatabase];
    
    // 각 테이블의 데이터를 파일에 저장 (암호화하여)
    for (const auto& tablePair : db.tables) {
        const TableData& table = tablePair.second;
        
        stringstream dataStream;
        dataStream << "TABLE: " << table.schema.tableName << "\n";
        for (const auto& column : table.schema.columns) {
            dataStream << column << "\t";
        }
        dataStream << "\n";

        for (const auto& row : table.rows) {
            for (const auto& value : row) {
                dataStream << value << "\t";
            }
            dataStream << "\n";
        }
        dataStream << "\n";

        // AES 암호화하여 파일에 저장
        string encryptedData = aesEncrypt(dataStream.str());
        file << encryptedData;
    }

    file.close();
    cout << "Database " << currentDatabase << " committed to " << filename << " with AES encryption successfully.\n";
}

// .mydb 파일에서 데이터를 읽고 복호화하는 함수
void loadDatabase(const string& dbName) {
    string filename = dbName + ".mydb";
    ifstream file(filename, ios::binary); // 바이너리 모드로 파일 열기

    if (!file.is_open()) {
        cerr << "Error opening file for reading: " << filename << "\n";
        return;
    }

    stringstream encryptedStream;
    string line;
    while (getline(file, line)) {
        encryptedStream << line << "\n";
    }

    file.close();

    // AES 복호화
    string decryptedData = aesDecrypt(encryptedStream.str());

    // 복호화된 데이터를 메모리에 로드
    stringstream ss(decryptedData);
    string tableData;
    
    Database db;
    db.dbName = dbName;

    while (getline(ss, tableData)) {
        cout << "Loaded Data: " << tableData << endl;
    }

    databases[dbName] = db;
    currentDatabase = dbName;
    cout << "Database " << dbName << " loaded successfully.\n";
}

// 쿼리를 파싱하고 해당 기능을 호출하는 함수
void executeQuery(string query) {
    // 명령어 끝의 세미콜론 제거
    query.erase(remove(query.begin(), query.end(), ';'), query.end());

    istringstream ss(query);
    string command;
    ss >> command;

    if (command == "CREATE") {
        string type;
        ss >> type;
        if (type == "DATABASE") {
            createDatabase(query);
        } else if (type == "TABLE") {
            createTable(query);
        } else {
            cerr << "Unsupported CREATE command type: " << type << "\n";
        }
    } else if (command == "USE") {
        useDatabase(query);
    } else if (command == "INSERT") {
        insertIntoTable(query);
    } else if (command == "SELECT") {
        selectFromTable(query);
    } else if (command == "COMMIT") {
        commitDatabase();
    } else if (command == "LOAD") {
        string dbName;
        ss >> dbName;
        loadDatabase(dbName);
    } else {
        cerr << "Unsupported command: " << command << "\n";
    }
}

int main() {
    string query;
    while (true) {
        cout << "Enter SQL command (or 'exit' to quit): ";
        getline(cin, query);

        if (query == "exit") {
            break;
        }

        executeQuery(query);
    }

    return 0;
}
