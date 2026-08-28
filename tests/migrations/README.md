# Migration tests

通用 CI 会验证空库 `base → head` 和直接父 revision → `head`。

当 revision 包含数据回填、去重、拆表或关联关系迁移时，还必须增加数据库测试：先升级到该 revision 的父版本，插入旧结构样例数据，再升级到新 revision 并断言结果。仅执行空表 DDL 不能替代这类数据断言。
