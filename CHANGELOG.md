# Changelog

## [0.2.0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/compare/v0.1.0...v0.2.0) (2026-03-06)


### Features

* add pub.dev and Packagist registry crawlers ([98a53f0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/98a53f0fc4dd16b89ece21cd49cc53b3b3a20ace))
* add pub.dev and Packagist registry crawlers ([2a96bf3](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/2a96bf35ec8491045401c1cc426d801f417d1682))
* **db:** apply new revisions during container startup ([bae1000](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bae1000a5b1267f15504315a2be8e02da4ca3af2))
* **db:** create the initial db schema ([88514f9](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/88514f930bd9200737ca19df1ac8c02e44f72e89))
* **db:** implement the two-level data model in sqlalchemy ([0f0b751](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/0f0b7511ff67467c3e50580ad832cf9c2a7e47e0))
* **db:** persist submitted sboms ([c7f618d](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/c7f618d16e8b23b908ebb733386cc2e794c311a2))
* **SBOM:** store raw submissions in filesystem for local dev ([9877a41](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/9877a41176a029e62a3013e641abd257b84e622f))


### Bug Fixes

* **ci:** align pre-commit ruff to v0.15.2; accept PEP 758 bracketless except ([99069bf](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/99069bfe4c0a9e5175816c30f3cc754e9f018cdf))
* **config:** strip any query params for sqlalchemy ([ac3bf11](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/ac3bf1130a95b48317c90ecdd18c1d0ec893be4c))
* **db:** deduplicate multiple pinned versions ([968f7bc](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/968f7bc580ccf53bc1a72c43454b0ffa6fe68174))
* **db:** remove the reserved pg_ prefix ([244a309](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/244a309880e8eeb04b85826e669e665dc37f6056))
* print details for OIDC audience mismatch ([cf135f0](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/cf135f04a4cf0452b83cd633de02daac7b7483de))
* **test:** cannot assume env db url format; pubdev now gives download count ([76b91f9](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/76b91f9644f9ddad93804a3c7e951794baeeeff5))
* unwrap github sbom envelope ([626a118](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/626a118395398b282c178a282c5e2a40188c1b42))
* use last-30-day downloads for adoption_downloads per spec ([bdf98c1](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/bdf98c1d6280691aee3ce42bb151020cbb65f728))


### Documentation

* show pre-commit.ci status ([7fdd54f](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/7fdd54f7f402e6984deef892f813beab5442300a))
* update instructions ([de9620f](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/de9620f8350ed2edc0c7b80085b80c9eb787132d))

## 0.1.0 (2026-02-25)


### Features

* add basic cli arguments to the module ([63088f2](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/63088f24535b3c8e1258901157382e8d9a45b745))
* initialize alembic ([21b050a](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/21b050a73101595f072592daca9b9653b28c4ea5))
* scaffold docker deployment ([0911c58](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/0911c58e8293e362d7ed0e161244fed9d0ce98ae))


### Bug Fixes

* avoid catching base exception ([8875acf](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/8875acf64ddd17bc683fefc8772c8a141db0e32e))
* dynamically load app version from metadata ([53840c8](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/53840c8b38d901d3d54e4b8ae1c2109f082d6418))
* fail early when no db connection can be made ([94b7868](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/94b786821af1f4875fa9496169e2cdef8d5a5797))


### Dependencies

* upgrade all version constraints ([44c73f7](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/44c73f7fd52c6668ca17cb113b3c9b25c22c5315))


### Documentation

* add pre-commit install to setup ([2540862](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/2540862958b60a370944d1df6c2125b953f219df))
* add project-specific agent instructions ([7affac1](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/commit/7affac17248f52da50b196863aaa22ceb4480dd8))
