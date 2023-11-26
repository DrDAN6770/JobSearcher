-- CREATE INDEX idx_city ON JobsInfo(`縣市`);
-- CREATE INDEX idx_working_exp ON JobsInfo(`工作經歷`);
CREATE INDEX idx_city ON City(`city`);
CREATE INDEX idx_jobcategory ON JobCategory(`category`);