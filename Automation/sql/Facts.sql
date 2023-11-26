CREATE TABLE IF NOT EXISTS JobsInfo  (
    `更新日期` DATE,
    `職缺名稱` VARCHAR(50),
    `公司名稱` VARCHAR(50),
    `工作內容` TEXT,
    `職務類別` INT,
    `工作待遇` VARCHAR(50),
    `工作性質` INT,
    `縣市` INT,
    `上班地點` VARCHAR(50),
    `管理責任` INT,
    `出差外派` INT,
    `上班時段` VARCHAR(50),
    `休假制度` INT,
    `可上班日` INT,
    `需求人數` VARCHAR(50),
    `工作經歷` INT,
    `學歷要求` INT,
    `科系要求` VARCHAR(50),
    `語文條件` VARCHAR(50),
    `擅長工具` VARCHAR(500),
    `工作技能` VARCHAR(500),
    `其他要求` VARCHAR(500),
    `連結` TEXT,
    PRIMARY KEY (`連結`(255)),
    FOREIGN KEY (`職務類別`) REFERENCES JobCategory(id),
    FOREIGN KEY (`工作性質`) REFERENCES JobType(id),
    FOREIGN KEY (`縣市`) REFERENCES City(id),
    FOREIGN KEY (`管理責任`) REFERENCES ManagementResponsibility(id),
    FOREIGN KEY (`出差外派`) REFERENCES Business_trip(id),
    FOREIGN KEY (`休假制度`) REFERENCES HolidaySystem(id),
    FOREIGN KEY (`可上班日`) REFERENCES AvailableStartdate(id),
    FOREIGN KEY (`工作經歷`) REFERENCES WorkingEXP(id),
    FOREIGN KEY (`學歷要求`) REFERENCES Degree(id)
);
