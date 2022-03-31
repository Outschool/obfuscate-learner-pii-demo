DROP TABLE IF EXISTS outschool_learner_pii;
CREATE TABLE outschool_learner_pii(uid uuid, email_address text, personal_info text, secret_token uuid);
INSERT INTO  outschool_learner_pii(uid, email_address, personal_info, secret_token) values('8260c4a6-d9a7-40af-bdb1-7ada809faff6', 'myunobfuscatedemail@address.com', 'Hello, this my personal secret info', '7690f44b-e67f-4af6-a1bd-badb1f114ad2');
INSERT INTO  outschool_learner_pii(uid, email_address, personal_info, secret_token) values('017c9eae-cac8-4661-bc45-520a99f9bee3', 'saavylearner@email.com', 'My secret info 2', 'f7d501c8-c413-4ed5-adf9-e01ed40e88aa');
INSERT INTO  outschool_learner_pii(uid, email_address, personal_info, secret_token) values('336d74cd-b905-4029-932d-6016bbd923c7', 'lovestodraw@gmail.com', 'Personal info 3', '1e222c85-00eb-4258-b520-3795d6a1218e');
