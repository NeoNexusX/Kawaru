# 使用此文件更新子模块
set -e
git submodule update --init --recursive --depth 1   # 或不用 --depth

# 对每个需要稀疏的子模块重复
(cd external/countries-states-cities-database &&
 git sparse-checkout init --cone &&
 git sparse-checkout set json/)

echo "Sparse-checkout applied."