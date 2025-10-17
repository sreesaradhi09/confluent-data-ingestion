The official versioning feature within Workflows for Confluence is designed to give you precise control over selecting major or minor versions for a document, moving beyond Confluence's native versioning capabilities.

Here is an explanation of versioning in simple steps, based on how the official versioning feature is set up and used within a Confluence workflow:

### 1. Configure the Workflow (Prerequisite Steps)

Before applying versioning, the workflow must be structured to accommodate the official version actions:

*   **Include Key Statuses:** The workflow must contain two required items in a linear fashion:
    1.  The **Create Official Version** status.
    2.  The **Approve Official Version** status, which must always fall after the "Create Official Version" status.
*   **Start the Process:** The workflow typically begins in a state such as "draft".

### 2. Initiate Version Creation

When you are ready to update the document's official version:

*   **Move to Creation Status:** On the Confluence page, move the workflow status to **Create Official Version**.

### 3. Define the Version Type and Description

At the "Create Official Version" stage, you must determine the significance of the changes:

*   **Add a Description:** You have the ability to add a **version description** to explain the changes.
*   **Select Major or Minor:** Choose whether to **create major version** or **create minor version**:
    *   **Major Version:** Selected for big changes (e.g., moving from 1.0 to 2.0).
    *   **Minor Version:** Selected for minor changes (e.g., moving from 1.0 to 1.1).

### 4. Approve and Proceed

Once the description is added and the version type is selected:

*   **Confirm Selection:** The description becomes effectively locked in.
*   **Proceed:** You select to "go forward and proceed". This action moves the document to the next version (e.g., the next major version).

### 5. Final Progression and Verification

The document is officially versioned as the workflow completes:

*   **Progress to Final State:** The workflow will progress to the final stage, such as "published".
*   **Review History:** You can view the history within the workflow info to see that the official version has been created (e.g., 1.0). The official version description is also included here.
*   **Runs Side-by-Side:** This official versioning operates side by side with Confluence's native versioning.


The official versioning feature in Workflows for Confluence gives users the ability to select between major or minor versions for a document, which goes beyond Confluence's nativeThe official versioning feature in Workflows for Confluence gives users the ability to select between major or minor versions for a document, which goes beyond Confluence's native versioning capabilities. This selection depends on the extent of the changes made to the document.

Here is an explanation of major and minor versioning based on the source material:

### Major Versioning

A **major version** is selected when there have been **big changes** made to a document.

*   **Example:** If you select the "create major version" option, the document version might change from **1.0 to 2.0**.
*   **Application:** When applying the workflow, you would move the document to the "create official version" status. At this point, you have the ability to select "create major version".
*   **Result:** Selecting this option and proceeding moves the document to the next major version.

### Minor Versioning

A **minor version** is selected when there have been **minor changes** made to the document.

*   **Example:** If you select the "create minor version" option, the document version might change from **1.0 to 1.1**.
*   **Application:** Like major versioning, the option to select "create minor version" is available when the document is in the "create official version" status within the workflow.

### Context of Use

Regardless of whether you choose a major or minor version, the process involves these steps within the workflow:

1.  The workflow must include a "Create Official Version" status, followed by an "Approve Official Version" status.
2.  Once the document reaches the "Create Official Version" status, you can add a **version description** and select whether to "create major version" or "create minor version".
3.  Once the description is added and the selection is made, the description is "effectively locked in".
4.  You then proceed forward, and this action moves the document to the next chosen version (major or minor).
5.  The official versioning information, including the description, is visible within the workflow history and runs "side by side" with Confluence's native versioning.



This content is designed for two PowerPoint slides to explain the core concepts and mechanics of the official versioning feature in Workflows for Confluence, based entirely on the provided sources.

***

## Slide 1: Understanding Official Versioning

**Title:** Official Versioning: Controlling Major and Minor Changes

### 1. What It Is
*   The official versioning feature in Workflows for Confluence **goes beyond Confluence's native versioning**.
*   It gives users the ability to explicitly select **major or minor versions** for a document.
*   The selection depends on the **changes that have been made** to the document.

### 2. Major Versioning
*   Used for **big changes**.
*   Selecting "create major version" moves the document to the next whole number version.
*   **Example:** Moving from version **1.0 to 2.0**.

### 3. Minor Versioning
*   Used for **minor changes**.
*   Selecting "create minor version" results in an incremental increase in the decimal version.
*   **Example:** Moving from version **1.0 to 1.1**.

### 4. Visibility
*   This official versioning runs **side by side** with Confluence’s native versioning.
*   The final official version (e.g., 1.0) and the **official version description** are visible within the workflow history.

***

## Slide 2: Implementation in the Workflow

**Title:** Implementing Official Versioning: Steps and Statuses

### 1. Workflow Setup (Prerequisite)
The workflow must include these two stages in a **linear fashion**:
*   **Create Official Version** status.
*   **Approve Official Version** status (This must **always fall after** the "Create Official Version" status).

### 2. The Versioning Action
Once the document is ready to be officially versioned (e.g., moving from a "draft" state):
*   **Move Status:** The user progresses the document to the **Create Official Version** status.
*   **Add Details:** At this stage, the user must:
    *   Add a **version description**.
    *   Select either **create major version** or **create minor version**.

### 3. Finalizing the Version
*   Once the version description and major/minor selection are made, the description is **effectively locked in**.
*   The user then selects to **"go forward and proceed"**.
*   This action moves the document to the next major or minor version and progresses the workflow (e.g., to "published").
*   The workflow history will show that the official version has been created, along with the included description.