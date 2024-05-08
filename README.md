# Big Data Project

- Work Presented by `Yassine Belkhadem` and `Hani Haddad` 


This project is group project for the Big Data class GL4 - 2024 organized by Mrs Lilia Sfaxi. 


The project aims to collect data about job postings from different sources such as Linkedin, Twitter and on the net. It helps getting insights out of the gathered data. These insights are both Real-Time and Cold Data. 

Here is the architecture of the app ( As far as the TP 2 additions ) 

![Image](./_assets/Architecture%20Big%20Data.png)


Now we aim to have such a dashboard as a final display: 



and thus the metrics we need to collect. 


## Metrics and Values


 | Metric Name                        | Description                 | How it will be calculated                    | Used dataset                                                                    |
 | ---------------------------------- | --------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------- |
 | Average of the remote jobs allowed | -                           | Number of remote Jobs / Total Number of jobs | `job_postings.csv`                                                              |
 | Jobs / Company                     | Number of jobs per company  | Number of Jobs for company X                 | `linkeding_tech_jobs.csv`, `data.csv`                                           |
 | Number Jobs per Location           | Number of jobs per Location | Number of jobs for Location X                | `job_postings.csv`, `linkedin_tech_jobs.csv`, `job_data_merged.csv`, `data.csv` |
 | Skills of Jobs                     | All skills keywords         | Tokenizing Skills                            | `job_postings.csv`, `job_postings.csv`, `data.csv`                              |
 | Average Salary per job             |



## Data Layout 

In this section we outline the schema of each data source. 


### [Linkedin Job Postings 2023](./datasets/job_postings.csv)

- also [Skills](./datasets/skills.csv)
- also [Industries](./datasets/industries.csv)

https://www.kaggle.com/datasets/arshkon/linkedin-job-postings

`job_id,company_id,title,description,max_salary,med_salary,min_salary,pay_period,formatted_work_type,location,applies,original_listed_time,remote_allowed,views,job_posting_url,application_url,application_type,expiry,closed_time,formatted_experience_level,skills_desc,listed_time,posting_domain,sponsored,work_type,currency,compensation_type,scraped`

### [Linkedin Tech Jobs](./datasets/linkedin_tech_jobs.csv)

https://www.kaggle.com/datasets/joebeachcapital/linkedin-jobs

`Company_Name,Class,Designation,Location,Total_applicants,LinkedIn_Followers,Level,Involvement,Employee_count,Industry,PYTHON,C++,JAVA,HADOOP,SCALA,FLASK,PANDAS,SPARK,NUMPY,PHP,SQL,MYSQL,CSS,MONGODB,NLTK,TENSORFLOW,LINUX,RUBY,JAVASCRIPT,DJANGO,REACT,REACTJS,AI,UI,TABLEAU,NODEJS,EXCEL,POWER BI,SELENIUM,HTML,ML`


###  [NYC Job Posting](./datasets/nyc-jobs.csv)

https://www.kaggle.com/datasets/new-york-city/new-york-city-current-job-postings/data

`Job ID,Agency,Posting Type,# Of Positions,Business Title,Civil Service Title,Title Code No,Level,Job Category,Full-Time/Part-Time indicator,Salary`

## [Job Posting Dataset](./datasets/job_data_merged.csv)

- https://www.kaggle.com/datasets/moyukhbiswas/job-postings-dataset

`,Category,Workplace,Location,Department,Type`

### [Online Job Posting](./datasets/data.csv)

https://www.kaggle.com/datasets/madhab/jobposts

`"jobpost","date","Title","Company","AnnouncementCode","Term","Eligibility","Audience","StartDate","Duration","Location","JobDescription","JobRequirment","RequiredQual","Salary","ApplicationP","OpeningDate","Deadline","Notes","AboutC","Attach","Year","Month","IT"`

## Links

https://www.tweepy.org/

https://www.figma.com/file/mdUKKaUp1ryHJPNsVDnCkU/Architecture-Big-Data?type=whiteboard&node-id=0%3A1&t=FIU3ismwkiteAQYB-1

