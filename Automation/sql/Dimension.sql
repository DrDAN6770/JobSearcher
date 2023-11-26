-- 可上班時間
CREATE TABLE AvailableStartdate(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    AvailableType VARCHAR(10)
);

-- 出差時間
CREATE TABLE Business_trip(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(20)
);

-- 縣市
CREATE TABLE City(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    city VARCHAR(10)
);

-- 學位要求
CREATE TABLE Degree(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    degree VARCHAR(10)
);

-- 科系要求
CREATE TABLE Department(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    department VARCHAR(10)
);

-- 休假
CREATE TABLE HolidaySystem(
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    Holiday_type VARCHAR(10)
);

-- 職業分類
CREATE TABLE JobCategory(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    category VARCHAR(10)
);

-- 工作型態
CREATE TABLE `JobType`(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    type VARCHAR(10)
);

-- 管理責任
CREATE TABLE ManagementResponsibility(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    management VARCHAR(10)
);

-- 工作經驗
CREATE TABLE WorkingEXP(  
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    yearexp VARCHAR(10)
);