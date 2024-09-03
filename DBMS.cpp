#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <fstream>
#include <iomanip>

using namespace std;

// 구조체를 사용하여 테이블 스키마 정의
struct TableSchema {
    string tableName;
    vector<string> columns;
    vector<string> columnTypes;
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

// 문자열이 숫자인지 확인하는 함수
bool isNumeric(const string& str) {
    return all_of(str.begin(), str.end(), ::isdigit);
}

// 논리 연산자를 통한 조건 평가 함수
bool evaluateCondition(const string& lhs, const string& op, const string& rhs) {
    if (op == "=") return lhs == rhs;
    if (op == "<") return isNumeric(lhs) && isNumeric(rhs) && stoi(lhs) < stoi(rhs);
    if (op == ">") return isNumeric(lhs) && isNumeric(rhs) && stoi(lhs) > stoi(rhs);
    if (op == "<=") return isNumeric(lhs) && isNumeric(rhs) && stoi(lhs) <= stoi(rhs);
    if (op == ">=") return isNumeric(lhs) && isNumeric(rhs) && stoi(lhs) >= stoi(rhs);
    if (op == "<>") return lhs != rhs;
    return false;
}

// CREATE DATABASE 쿼리를 처리하는 함수
void createDatabase(const string& query) {
    istringstream ss(query);
    string token;
    ss >> token; // 'CREATE'
    ss >> token; // 'DATABASE'

    string dbName;
    ss >> dbName; // 데이터베이스 이름

    if (databases.find(dbName) != databases.end()) {
        cerr << "Database: " << dbName << "는 이미 존재하는 데이터베이스 입니다.\n";
        return;
    }

    // 데이터베이스 파일 생성
    string filename = dbName + ".mydb";
    ofstream file(filename);
    if (!file.is_open()) {
        cerr << "데이터베이스 파일: " << filename << "을 생성하는데 실패했습니다. \n";
        return;
    }
    file.close(); // 파일을 생성만 하고 닫음

    Database db;
    db.dbName = dbName;
    databases[dbName] = db;

    cout << "Database: " << dbName << "를 생성 완료하였습니다. " << filename << "파일 생성완료.\n";
}

// .mydb 파일에서 데이터를 읽어와 메모리에 로드하는 함수
void loadDatabase(const string& dbName) {
    string filename = dbName + ".mydb";
    ifstream file(filename); // 텍스트 모드로 파일 열기

    if (!file.is_open()) {
        cerr << "파일: " << filename << "을 읽어오는데 실패하였습니다. \n";
        return;
    }

    stringstream ss;
    string line;
    while (getline(file, line)) {
        ss << line << "\n";
    }

    file.close();

    // 데이터를 메모리에 로드
    string tableData;
    Database db;
    db.dbName = dbName;
    string currentTableName;
    TableSchema currentSchema;

    while (getline(ss, tableData)) {
        if (tableData.find("TABLE:") != string::npos) {
            currentTableName = tableData.substr(7); // "TABLE: " 이후의 테이블 이름 추출
            db.tables[currentTableName] = TableData{TableSchema{currentTableName, {}, {}}, {}};
            currentSchema = db.tables[currentTableName].schema; // 현재 테이블 스키마 초기화
        } else if (tableData.find("SCHEMA:") != string::npos) {
            string schemaLine = tableData.substr(7); // "SCHEMA:" 이후의 스키마 정의
            istringstream schemaStream(schemaLine);
            string columnDef;

            while (getline(schemaStream, columnDef, ',')) {
                size_t typeStart = columnDef.find('(');
                size_t typeEnd = columnDef.find(')');
                if (typeStart != string::npos && typeEnd != string::npos && typeEnd > typeStart + 1) {
                    string columnType = columnDef.substr(typeStart + 1, typeEnd - typeStart - 1); // 데이터 타입
                    string columnName = columnDef.substr(typeEnd + 1);  // 데이터 타입 이후의 열 이름
                    columnName.erase(remove(columnName.begin(), columnName.end(), ' '), columnName.end());
                    columnType.erase(remove(columnType.begin(), columnType.end(), ' '), columnType.end());
                    currentSchema.columns.push_back(columnName);
                    currentSchema.columnTypes.push_back(columnType);
                }
            }

            db.tables[currentTableName].schema = currentSchema;
        } else if (!currentTableName.empty() && db.tables.find(currentTableName) != db.tables.end()) {
            istringstream rowStream(tableData);
            vector<string> row;
            string value;

            while (rowStream >> value) {
                row.push_back(value);
            }

            if (!row.empty() && row.size() == currentSchema.columns.size()) {
                db.tables[currentTableName].rows.push_back(row);
            }
        }
    }

    databases[dbName] = db;
    currentDatabase = dbName;
    cout << "Database " << dbName << "로드 완료. \n";
}

// USE DATABASE 쿼리를 처리하는 함수
void useDatabase(const string& query) {
    istringstream ss(query);
    string token;
    ss >> token; // 'USE'

    string dbName;
    ss >> dbName; // 데이터베이스 이름

    if (databases.find(dbName) != databases.end()) {
        // 이미 메모리에 로드된 데이터베이스 사용
        currentDatabase = dbName;
        cout << "Database: " << dbName << "를 사용합니다. \n";
    } else {
        // 파일에서 데이터베이스 로드 시도
        loadDatabase(dbName);
    }
}

// CREATE TABLE 쿼리를 처리하는 함수
void createTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "데이터베이스 선택 후 진행해주세요. \n";
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
        size_t typeStart = columnDef.find('(');
        size_t typeEnd = columnDef.find(')');
        if (typeStart != string::npos && typeEnd != string::npos && typeEnd > typeStart + 1) {
            string columnType = columnDef.substr(typeStart + 1, typeEnd - typeStart - 1); // 데이터 타입
            string columnName = columnDef.substr(typeEnd + 1);  // 데이터 타입 이후의 열 이름
            
            // 공백 제거
            cout << "공백 제거 전 이름: " << columnName << " 타입: " << columnType << endl;
            columnName.erase(remove(columnName.begin(), columnName.end(), ','), columnName.end());
            columnType.erase(remove(columnType.begin(), columnType.end(), ' '), columnType.end());
            cout << "공백 제거 후 이름: " << columnName << " 타입: " << columnType << endl;
            
            schema.columns.push_back(columnName);
            schema.columnTypes.push_back(columnType);
        } else {
            cerr << "ERROR: 잘못된 테이블 정의입니다. 형식은 다음과 같아야 합니다: (type)column_name\n";
            return;
        }
    }

    // 테이블을 데이터베이스에 추가
    TableData table;
    table.schema = schema;
    databases[currentDatabase].tables[schema.tableName] = table;

    cout << "Table: " << schema.tableName << " 테이블 생성이 완료되었습니다. 현재 데이터베이스: " << currentDatabase << ".\n";
}

// INSERT INTO 쿼리를 처리하는 함수
void insertIntoTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "데이터베이스 선택 후 진행해주세요. \n";
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
        cerr << "ERROR: " << tableName << " 존재하지 않습니다. 현재 데이터베이스: " << currentDatabase << ".\n";
        return;
    }

    TableData& table = it->second;
    vector<string> row;
    string value;
    int columnIndex = 0; // `columnIndex` 변수를 선언하고 0으로 초기화

    while (ss >> value) {
        // 데이터 타입 검사
        if (columnIndex >= table.schema.columnTypes.size()) {
            cerr << "Error: 입력된 데이터 수가 테이블의 열 수를 초과했습니다. 테이블: " << tableName << ".\n";
            return;
        }

        string expectedType = table.schema.columnTypes[columnIndex];

        // 데이터 타입 검사
        bool typeMismatch = false;

        if (expectedType == "int") {
            // 숫자가 아닌 경우 에러
            if (!isNumeric(value)) {
                typeMismatch = true;
            }
        } else if (expectedType == "float") {
            // 실수가 아닌 경우 에러
            if (value.find_first_not_of("0123456789.") != string::npos || count(value.begin(), value.end(), '.') > 1) {
                typeMismatch = true;
            }
        } else if (expectedType == "string") {
            // 문자열은 따옴표로 감싸져 있어야 함
            if (value.front() != '"' || value.back() != '"') {
                typeMismatch = true;
            }
        } else if (expectedType == "date") {
            // 날짜는 'YYYY-MM-DD' 형식이어야 함
            if (value.find_first_not_of("0123456789-") != string::npos || count(value.begin(), value.end(), '-') != 2) {
                typeMismatch = true;
            }
        }

        if (typeMismatch) {
            cerr << "Error: 데이터 타입이 일치하지 않습니다. 열: " << table.schema.columns[columnIndex]
                 << ", 예상 타입: " << expectedType << ", 제공된 값: " << value << ".\n";
            return;
        }

        row.push_back(value);
        columnIndex++;
    }

    // 입력된 값의 개수와 테이블의 열 수가 일치하는지 확인
    if (row.size() != table.schema.columns.size()) {
        cerr << "Error: 입력 값 (" << row.size() << ")개, 컬럼 갯수가 일치 하지 않습니다. 컬럼 (" << table.schema.columns.size() << ")개 테이블 명: " << tableName << ".\n";
        return;
    }

    table.rows.push_back(row);
    cout << "Success: 데이터 삽입 성공 " << tableName << ", 데이터베이스: " << currentDatabase << "\n";
}

// DELETE 쿼리를 처리하는 함수
void deleteFromTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "데이터베이스 선택 후 진행해주세요. \n";
        return;
    }

    istringstream ss(query);
    string token;
    ss >> token; // 'DELETE'
    ss >> token; // 'FROM'

    string tableName;
    ss >> tableName; // 테이블 이름

    // 세미콜론 제거
    tableName.erase(remove(tableName.begin(), tableName.end(), ';'), tableName.end());

    auto it = databases[currentDatabase].tables.find(tableName);
    if (it == databases[currentDatabase].tables.end()) {
        cerr << "ERROR: " << tableName << " 테이블이 존재하지 않습니다. 현재데이터베이스: " << currentDatabase << ".\n";
        return;
    }

    TableData& table = it->second;

    // WHERE 절이 있는지 확인
    size_t wherePos = query.find("WHERE");
    if (wherePos != string::npos) {
        string whereClause = query.substr(wherePos + 6); // 'WHERE ' 이후의 문자열

        // WHERE 절 조건 처리
        string whereColumn, op, whereValue;
        istringstream whereStream(whereClause);
        whereStream >> whereColumn >> op;
        getline(whereStream, whereValue);
        whereValue.erase(remove(whereValue.begin(), whereValue.end(), ' '), whereValue.end());

        // 문자열 값에서 작은 따옴표 제거
        if (whereValue.front() == '\'' && whereValue.back() == '\'') {
            whereValue = whereValue.substr(1, whereValue.size() - 2);
        }

        if (whereColumn.empty() || op.empty() || whereValue.empty()) {
            cerr << "Invalid DELETE query syntax. Use DELETE FROM table_name WHERE column operator value;\n";
            return;
        }

        // 선택한 열의 인덱스를 찾기 위한 맵
        unordered_map<string, int> columnIndices;
        for (int i = 0; i < table.schema.columns.size(); ++i) {
            columnIndices[table.schema.columns[i]] = i;
        }

        int whereColumnIndex = -1;
        if (columnIndices.find(whereColumn) != columnIndices.end()) {
            whereColumnIndex = columnIndices[whereColumn];
        } else {
            cerr << "ERROR: 테이블에 " << whereColumn << " 컬럼이 존재하지 않습니다. " << tableName << ".\n";
            return;
        }

        // 조건에 맞는 행을 삭제
        table.rows.erase(remove_if(table.rows.begin(), table.rows.end(), [&](const vector<string>& row) {
            return evaluateCondition(row[whereColumnIndex], op, whereValue);
        }), table.rows.end());

        cout << "Rows 삭제 완료, from " << tableName << " where " << whereColumn << " " << op << " '" << whereValue << "' successfully in database " << currentDatabase << ".\n";
    } else {
        // WHERE 절이 없으면 경고 메시지 표시하고 삭제를 수행하지 않음
        cerr << "Invalid DELETE query syntax. Missing WHERE clause. Use DELETE FROM table_name WHERE column operator value;\n";
    }
}

// SELECT 쿼리를 처리하는 함수 (WHERE 조건 및 여러 열 조회 추가)
void selectFromTable(const string& query) {
    if (currentDatabase.empty()) {
        cerr << "데이터베이스 선택 후 진행해주세요.\n";
        return;
    }

    istringstream ss(query);
    string token;
    ss >> token; // 'SELECT'

    vector<string> selectColumns;
    string column;

    // SELECT 절에서 조회할 열 추출
    while (ss >> column) {
        if (column == "FROM") {
            break; // 'FROM' 키워드를 만나면 루프 종료
        } else if (column == "*") {
            selectColumns.clear(); // 모든 열을 선택하기 위해 초기화
            ss >> column;  // Move to the next token (should be 'FROM')
            break;  // Exit loop to process 'FROM'
        } else {
            // 쉼표로 구분된 여러 열을 처리
            size_t commaPos;
            while ((commaPos = column.find(' ')) != string::npos) {
                string colName = column.substr(0, commaPos);
                cout << "col: " << colName << endl;
                colName.erase(remove(colName.begin(), colName.end(), ' '), colName.end());
                selectColumns.push_back(colName);
                column.erase(0, commaPos + 1);
                column.erase(remove(column.begin(), column.end(), ' '), column.end());
            }
            column.erase(remove(column.begin(), column.end(), ' '), column.end());
            selectColumns.push_back(column);
        }
    }

    if (column != "FROM") {
        cerr << "SQL 구문 오류: 'FROM' 키워드가 누락되었습니다.\n";
        return;
    }

    string tableName;
    ss >> tableName; // 테이블 이름을 읽음

    // 세미콜론 제거
    tableName.erase(remove(tableName.begin(), tableName.end(), ';'), tableName.end());

    auto it = databases[currentDatabase].tables.find(tableName);
    if (it == databases[currentDatabase].tables.end()) {
        cerr << "ERROR: " << tableName << "이 데이터베이스 " << currentDatabase << "에 존재하지 않습니다.\n";
        return;
    }

    TableData& table = it->second;

    // 모든 열 선택하는 경우 처리
    if (selectColumns.empty() || (selectColumns.size() == 1 && selectColumns[0] == "*")) {
        selectColumns = table.schema.columns; // 현재 테이블의 모든 열 선택
    }

    // WHERE 절이 있는지 확인
    string whereClause;
    size_t wherePos = query.find("WHERE");
    if (wherePos != string::npos) {
        whereClause = query.substr(wherePos + 6); // 'WHERE ' 이후의 문자열
    }

    // WHERE 절 조건 처리
    string whereColumn, op, whereValue;
    if (!whereClause.empty()) {
        istringstream whereStream(whereClause);
        whereStream >> whereColumn >> op >> whereValue;
    }

    // 선택한 열의 인덱스를 찾기 위한 맵
    unordered_map<string, int> columnIndices;
    for (int i = 0; i < table.schema.columns.size(); ++i) {
        columnIndices[table.schema.columns[i]] = i;
    }

    // 선택한 열 출력
    for (const auto& col : selectColumns) {
        if (columnIndices.find(col) == columnIndices.end()) {
            cerr << "ERROR: " << col << " 컬럼이 테이블 " << tableName << "에 존재하지 않습니다.\n";
            return;  // 존재하지 않는 열이면 오류 출력 후 반환
        }
        cout << col << "\t";
    }
    cout << "\n";

    int whereColumnIndex = -1;
    if (!whereColumn.empty() && columnIndices.find(whereColumn) != columnIndices.end()) {
        whereColumnIndex = columnIndices[whereColumn];
    }

    for (const auto& row : table.rows) {
        if (row.empty()) {
            continue;  // 빈 행은 무시
        }

        if (whereColumnIndex == -1 || (whereColumnIndex != -1 && whereColumnIndex < row.size() && evaluateCondition(row[whereColumnIndex], op, whereValue))) {
            for (const auto& col : selectColumns) {
                int colIndex = columnIndices[col];
                if (colIndex < row.size()) {
                    cout << row[colIndex] << "\t";
                } else {
                    cerr << "Invalid column index: " << colIndex << " for row size " << row.size() << ".\n";
                    return;
                }
            }
            cout << "\n";
        }
    }
}

// COMMIT 쿼리를 처리하고 데이터를 파일에 저장하는 함수
void commitDatabase() {
    if (currentDatabase.empty()) {
        cerr << "데이터베이스 선택 후 진행해주세요. \n";
        return;
    }

    string filename = currentDatabase + ".mydb";
    ofstream file(filename); // 파일을 텍스트 모드로 열기

    if (!file.is_open()) {
        cerr << "Error: " << filename << "파일을 쓰는데 실패했습니다. \n";
        return;
    }

    Database& db = databases[currentDatabase];

    // 각 테이블의 데이터를 파일에 저장
    for (const auto& tablePair : db.tables) {
        const TableData& table = tablePair.second;

        stringstream dataStream;
        dataStream << "TABLE: " << table.schema.tableName << "\n";
        
        // 스키마 정보 저장
        dataStream << "SCHEMA:";
        for (size_t i = 0; i < table.schema.columns.size(); ++i) {
            dataStream << " (" << table.schema.columnTypes[i] << ")" << table.schema.columns[i];
        }
        dataStream << "\n";

        // 데이터 저장
        for (const auto& row : table.rows) {
            for (const auto& value : row) {
                dataStream << value << "\t";
            }
            dataStream << "\n";
        }
        dataStream << "\n";

        // 파일에 저장
        file << dataStream.str();
    }

    file.close();
    cout << "Database " << currentDatabase << " committed 완료, " << filename << " 파일에 쓰기 및 저장 완료되었습니다. \n";
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
    } else if (command == "DELETE") {
        deleteFromTable(query);
    } else if (command == "COMMIT") {
        commitDatabase();
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
