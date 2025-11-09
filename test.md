# 第一次尝试

## git配置

### 先将师兄的仓库fork到本地的仓库中

### 在本地仓库建立feature分支，然后克隆到本地

进入到合适的路径中，然后git clone https://github.com/Bug-free-Bear-code1/Kawaru 

### *在本地建立分支feature_test和远程feature分支关联起来*

*git checkout -b feature_test origin/feature*

### 上面这个操作是错误的，后面我push到自己的远程仓库发现多了一个feature_test分支 

修改为：git checkout -b feature origin/feature  这样本地的featrue分支才和远程的feature分支对应起来

### 使用 -vv 命令可以查看远程关联关系

git  branch -vv

## 个人环境

编译器：cursor

环境：使用 conda 建立的虚拟环境，里面有很多自己导入的包

### 后续

在本地跑发现缺失很多包，正在导包中，先拿这个做一次简单的pr尝试

