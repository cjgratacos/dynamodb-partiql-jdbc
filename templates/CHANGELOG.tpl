# {{projectName}} {{projectVersion}}

{{#if releaseNotesUrl}}[Release Notes]({{releaseNotesUrl}}) | {{/if}}[GitHub Release]({{releaseUrl}})

{{#if hasChanges}}
## What's Changed
{{#each commitsByType}}
### {{title}}
{{#each commits}}
- {{#if scope}}**{{scope}}:** {{/if}}{{#if subject}}{{{subject}}}{{else}}{{{messageTitle}}}{{/if}} {{#if issues}}[`#{{issues}}`]({{repoUrl}}/issues/{{issues}}){{/if}} @{{commitAuthor}}
{{/each}}
{{/each}}

{{#if commitCount}}
**Full Changelog**: {{repoUrl}}/compare/{{previousTag}}...{{tagName}}
{{/if}}
{{/if}}

{{#if contributors}}
## Contributors
{{#each contributors}}
- {{name}}{{#if login}} ([@{{login}}]({{#if url}}{{url}}{{else}}{{../repoUrl}}/{{login}}{{/if}})){{/if}}
{{/each}}
{{/if}}
