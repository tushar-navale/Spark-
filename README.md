# Spark-
Using Pyspark for high density streaming data in real time

the reference link for the project : https://hackmd.io/@pesu-bigdata/ryDbjmTkke#Task-1---Spark

Problem Statement

The International Olympic Committee is preparing for their centennial celebration and wants to recognize outstanding athletes and coaches from the past decade. They've asked you, as a data analyst, to help them identify the top performers based on the data from 2012, 2016, and 2020 Olympics.
Dataset

Dataset to test your code

You are provided with 2 folders in the above link.

    Small Dataset : This dataset can be used to understand how the points tallying works and users should approach the problem
    Large Dataset : This will simulate the large dataset you will be provided on the portal. Test your code with this dataset to check how its performing with space and time complexity.

However, keep in mind that the final dataset on the portal is larger than what is given to you and there is no guarantee that if you pass the local large dataset, you will pass on the portal. Understading the problem and the constraints is key to get the right solution.
Dataset Description

    You are provided with 3 datasets for the athletes for each year 2012,2016,2020 that contains the following columns :

Column 	Description
id 	A unique ID for each athlete
name 	The name of the athlete
dob 	Date of Birth
height(m) 	Height in meters
weight(kg) 	Weight in kilograms
sport 	The sport the athlete participated in
event 	The event the athlete participated in his respective sport
country 	The nationality of the athlete
num_followers 	Number of followers the athlete has
num_articles 	Number of articles published under the athlete names
personal_best 	The athlete's best performance for the sport on/before that respective year
coach_id 	The athlete's coach for that year

    You are provided with 1 dataset for the coaches over the years 2012,2016,2020 that contains the following columns:

Column 	Description
id 	The unique ID for the coach
name 	The name of the coach
age 	Age of the coach
years_of_experience 	How many years of experience the coach has
sport 	The sport the coach trains athletes for
country 	The nationality of the coach
contract_id 	The unique ID of the coach's contract for the tenure of his coaching
certification_committee 	The committee under which the coach obtained certification

    Finally, one dataset to cover the medals obtained by the athletes over the years 2012,2016,2020. It contains the following columns:

Column 	Description
id 	The unique ID for the athlete who won the medal
sport 	Sport under which the medal was won
event 	The event under which the medal was obtained
year 	The year of winning the medal
country 	The country that hosted the Olympics for that year
medal 	The category of the medal won (Gold/Silver/Bronze)
Task 1.1: Identifying the All-Time Best Athletes
Description:

The IOC wants to honor the best athlete in each sport across all three Olympic years (2012, 2016, and 2020). Your job is to analyze the data and determine which athlete has performed the best in their respective sport over this period. Consider the following table to measure the total points for each athlete over all three olympics and rank them on that metric.
Scoring Metrics:

2012 :
Medal Category 	Points
Gold 	20
Silver 	15
Bronze 	10

What the above table describes:
1 gold medal is worth 20 points in 2012. Similarly in 2012, 1 silver is worth 15 points and 1 bronze is worth 10 points.

2016 :
Medal Category 	Points
Gold 	12
Silver 	8
Bronze 	6

2020 :
Medal Category 	Points
Gold 	15
Silver 	12
Bronze 	7

NOTE : Best performance is considered as the most points won.
Task 1.2: Recognizing Top International Coaches
Description:

To promote international cooperation, the IOC wants to acknowledge coaches who have successfully trained athletes from different countries. They've asked you to focus on three specific countries: China, India, USA and identify the top 5 coaches who have trained athletes from these nations. Your analysis should consider and rank coaches by aggregating the total points won by the athletes under them. For this, you may refer to the same scoring metric given above.

The OUTPUT should all be in uppercase.
Points to keep in mind while solving the Task

Here are some points to keep in mind about the datasets provided :

    A coach teaches only 1 country per year. He cannot coach multiple countries simultaneously but keep in mind, he can change countries over the years i.e. coach country X in 2012 and a different country Y in 2016.
    The sport of the athlete must match the sport of the coach.
    The data provided you can be dirty and have years we don’t want in the final answer. The only years to be considered for the analysis are 2012,2016,2020. All other years are null and void.
    ⁠The dataset has random casing. Fix the randomness by converting all to uppercase.
    In case of a tie on total points, the athlete/coach with more gold medals comes first. If number of golds are same, consider silver and so on. If two or more coaches have the same number of points, gold, silver and bronze medals then pick the athlete/coach with the lexicographic lowest name.

NOTE : The lowest lexicographic name is the first name when all the names are arranged alphabetically or are in dictionary order.
Working with the files

Working with CSV Files in PySpark :

    PySpark’s read.csv() is the preferred method to read the files.
    An example to import the csv is as follows :
    athlete_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
    Explaination :

    ```
    header=True: This ensures that the first row of the CSV is treated as column headers.
    inferSchema=True: This automatically infers the data type of each column.
    ```

    After loading the datasets, you can perform various transformations, filters, or joins to process the data as needed.

    If you wish to see the whole csv, you can store it using write.csv(output_path, header=True, mode="__")

    The expected output however requires only the name, so extract only that column and write it to a txt file. The text file is expected to contain the names seperated with ',' in the following format and ensure there are no extra whitespaces

    The command to execute the python file should take this structure:
    python task1.py athletes_2012.csv athletes_2016.csv athletes_2020.csv coaches.csv medals.csv output.txt
    where, output.txt will hold you final output with the structure shown above

Students are expected to submit one input code file which outputs one .txt file with the answer to both tasks

We expect you to output your final answer into the txt file in the following format
([task1.1 comma seperated values],[task1.2 comma seperated values])
Example:
([athlete A,athlete B,....],[coach A,coach B,...])

When writing [task1.1 comma seperated values] write the athlete names sorted according to sport alphabetically

Similarly,
When writing [task1.2 comma seperated values] write the top 5 coaches of China first, then India, then USA
