 --Ensures no duplicate users exist, present just in case. Will delete later when confirmation of proper entry.
DELETE FROM tw_user WHERE rowid NOT IN (SELECT min(rowid) FROM tw_user GROUP BY user_id);