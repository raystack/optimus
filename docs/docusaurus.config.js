const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Optimus',
  tagline: 'Performant data workflow orchestrator',
  url: 'https://odpf.github.io',
  baseUrl: '/optimus/',
  onBrokenLinks: 'throw',
  trailingSlash: true,
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'odpf',
  projectName: 'optimus',
  themeConfig: {
    navbar: {
      title: 'Optimus',
      logo: {
        alt: 'Optimus Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          href: 'https://github.com/odpf/optimus',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Documentation',
              to: '/',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Slack',
              href: 'https://join.slack.com/t/odpf-community/shared_invite/zt-qfokfy4j-30K9IzigF9Lsqpmt1Jen7Q',
            },
          ],
        },
        {
          title: 'More',
          items: [
            // {
            //   label: 'Blog',
            //   to: '/blog',
            // },
            {
              label: 'GitHub',
              href: 'https://github.com/odpf/optimus',
            },
          ],
        },
      ],
      copyright: `Copyright © 2020-${new Date().getFullYear()} ODPF`,
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          routeBasePath: '/',
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/odpf/optimus/edit/master/docs/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
