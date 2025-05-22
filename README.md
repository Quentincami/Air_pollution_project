This project is a personnal project that mix both of my interest, the environment and Data Science.
I am using OpenAQ data that controls air pollution all around the world to hopefully analyse and model Air Quality on 3 cities that are dear to me, Marseille, Lyon and Zurich.
I started by Zurich, but there is a lot of missing datas that render the time serie analysis quite difficult. So I switched to Lyon, and even though there is still some missing data, it's not as much as Zurich, and with some fillings we can manage it.
The steps to have a global file for a specific city are the following :
- Spot the sensor and their location_id related to the city you want to analyse
- Download all the daily files that are corresponding to these location_ids for each sensor. They are a file for each day, containing hourly values
- Unzip the files
- The original files have one row for each sensor and hour, so we transform the file into and indexed by timestamp with columns for each parameters measured.
- Create yearly files for each sensor
- Merge all yearly files to a global one.

Right now, the different scripts are manually launched inside a manually created EC2 instance. The goal is to automate the pipeline to have at least all this retrieve/modify csv automated, so I can focus on the analysis for when I want to start again with fresh datas.
