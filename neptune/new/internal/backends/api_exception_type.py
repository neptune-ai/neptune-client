import enum


class ApiExceptionType(enum.Enum):
    limit_of_projects_reached = "LIMIT_OF_PROJECTS_REACHED"
    limit_of_members_in_organization_reached = "LIMIT_OF_MEMBERS_IN_ORGANIZATION_REACHED"
    workspace_readonly = "WORKSPACE_READONLY"
    user_not_in_project = "USER_NOT_IN_PROJECT"
    service_account_already_exists_in_organization = (
        "SERVICE_ACCOUNT_ALREADY_EXISTS_IN_ORGANIZATION"
    )
    service_account_not_in_project = "SERVICE_ACCOUNT_NOT_IN_PROJECT"
    limit_of_service_accounts_in_organization_reached = (
        "LIMIT_OF_SERVICE_ACCOUNTS_IN_ORGANIZATION_REACHED"
    )
    cannot_add_members_to_private_individual_project = (
        "CANNOT_ADD_MEMBERS_TO_PRIVATE_INDIVIDUAL_PROJECT"
    )
    cannot_add_manager_to_individual_project = "CANNOT_ADD_MANAGER_TO_INDIVIDUAL_PROJECT"
    cannot_add_outsiders_to_team_project = "CANNOT_ADD_OUTSIDERS_TO_TEAM_PROJECT"
    cannot_leave_team_project = "CANNOT_LEAVE_TEAM_PROJECT"
    admin_cannot_leave_project = "ADMIN_CANNOT_LEAVE_PROJECT"
    limit_of_project_members_reached = "LIMIT_OF_PROJECT_MEMBERS_REACHED"
    visibility_restricted = "VISIBILITY_RESTRICTED"
    plan_not_available = "PLAN_NOT_AVAILABLE"
    payments_not_supported = "PAYMENTS_NOT_SUPPORTED"
