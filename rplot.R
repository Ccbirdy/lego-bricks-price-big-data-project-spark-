data<-read.csv("Gcore5.csv",header=T,sep=",",encoding="UTF-8")
head(data)
#can not scroll
#install.packages("scatterplot3d")
library(scatterplot3d)
scatterplot3d(data$memory,data$executor.cores,data$time,main="Benchmark Results",pch=16)
#can scroll
#install.packages("rgl")
library(rgl)
attach(data)
plot3d(data$memory,data$executor.cores,data$time,col="red",size=5)
