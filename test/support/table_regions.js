module.exports = {
  name: 'sqb_test.regions',
  createSql: [`
CREATE TABLE SQB_TEST.regions
(
  id    VARCHAR2(5),
  name  VARCHAR2(16)
)`, `
ALTER TABLE SQB_TEST.regions ADD (
  CONSTRAINT regions_PK
  PRIMARY KEY
  (id)
  ENABLE VALIDATE)
  `],
  insertSql: 'insert into sqb_test.regions (id,name) values (:ID,:Name)',
  rows: [
    {
      ID: 'FR',
      Name: 'FR Region'
    },
    {
      ID: 'TR',
      Name: 'TR Region'
    },
    {
      ID: 'GB',
      Name: 'GB Region'
    },
    {
      ID: 'US',
      Name: 'US Region'
    },
    {
      ID: 'CN',
      Name: 'CN Region'
    }
  ]
};
