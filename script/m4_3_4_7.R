modelId = c(11,217,27,216,17,210,14,211,218,26,28,24,220,21,25,15,221,222,29,23,19,16,223,219,116,224,22,31,125,13,111,18,33,119,333,112,322,311,318,110,319,34,12,124,120,121,325,331,329,334,346,328,32,320,341,348,340,323,337,343,345,336,312,321,335,347,344,342,316,315,36,115,114,113,122,317,212,313,35,314,39,338,225,310,324,332,327,37,38,330)
modelId = paste0("model_", modelId)
factorList = c("age","attr1","attr2","attr3","brand","cate","comment_num","has_bad_comment","sex","user_level")
predict = h2o.importFile("/home/wanghl/jd_contest/0519/4/predict_DL_0_1_4.csv")
predict[,factorList] = as.factor(predict[,factorList])
predict[,modelId] = as.factor(predict[,modelId])


data <- h2o.importFile("/home/wanghl/jd_contest/0519/4/data_DL_3_4_7.csv")
parts <- h2o.splitFrame(data, c(0.8))
train	<-parts[[1]]
test	<-parts[[2]]

nrow(data[data$target==1,]) / nrow(data[data$target==0,])
nrow(train[train$target==1,]) / nrow(train[train$target==0,])
nrow(test[test$target==1,]) / nrow(test[test$target==0,])

x = setdiff(names(data), "target")
train[,factorList] = as.factor(train[,factorList])
train[,modelId] = as.factor(train[,modelId])

y = c("target")
train[,y] = as.factor(train[,y])

test[,factorList] = as.factor(test[,factorList])
test[,y] = as.factor(test[,y])
test[,modelId] = as.factor(test[,modelId])

x = setdiff(x, "user_id")
x = setdiff(x, "sku_id")

m4_3_4_7 <- h2o.deeplearning(x, y, train, nfolds=5,
  model_id = "m4_3_4_7", hidden = c(300:400),
  activation = "RectifierWithDropout",
  l1 = 0.00001,
  l2 = 0.0001,
  input_dropout_ratio = 0.2,
  hidden_dropout_ratios = c(0.1, 0.1),
 replicate_training_data = TRUE,
 balance_classes = T, 
 class_sampling_factors=c(5, 1), 
 shuffle_training_data = T,
  classification_stop = -1,
  stopping_metric = "misclassification",
  stopping_tolerance = 0.001,
  stopping_rounds = 8,
  epochs = 500
  )

p4 = h2o.predict(m4_3_4_7, predict)
p4bind = h2o.cbind(predict$user_id,predict$sku_id,p4$predict,p4$p0)
p4df = as.data.frame(p4bind)
nrow(p4df[p4df$predict==0,])
result4 = p4df[p4df$predict==0,]
write.csv(result4, file = "/home/wanghl/jd_contest/0519/result/result_3_4_7.csv", row.names=FALSE, quote =FALSE)
