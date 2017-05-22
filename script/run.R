library(h2o)
h2o.init(nthreads=6,max_mem_size="28g")
setwd("/home/wanghl/jd_contest/0519/script/")
files=dir()
for(file in files){
    print(file)
    source(file)
}
