CREATE FUNCTION Security.tvf_securitypredicate(@UserName AS varchar(50), @ProjectID AS varchar(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS tvf_securitypredicate_result
WHERE 
    -- User1 chỉ được truy cập Project1
	@UserName = USER_NAME() OR
    (USER_NAME() = 'Giangntt@ierp.vn' AND @ProjectID = '320')
    -- User2 chỉ được truy cập Project2
    --OR (USER_NAME() = 'User2@contoso.com' AND @UserName = 'User2@contoso.com' AND @ProjectID = 'Project2');