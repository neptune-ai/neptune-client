from neptune.management import add_project_member

project_id = "main-team/complex-experiment"
api_tokenu = ""
api_tokensa = ""

lst = add_project_member(
    name=project_id, username="team-member", role="owner", api_token=api_tokensa
)

print(lst)
