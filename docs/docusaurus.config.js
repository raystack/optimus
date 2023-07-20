const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Optimus',
  tagline: 'Performant data workflow orchestrator',
  url: 'https://raystack.github.io',
  baseUrl: '/optimus/',
  onBrokenLinks: 'throw',
  // trailingSlash: true,
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'raystack',
  projectName: 'optimus',

  themeConfig: {
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
      // switchConfig: {
      //   darkIcon: '☾',
      //   lightIcon: '☀️',
      // },
    },
    navbar: {
      title: 'Optimus',
      logo: { src: 'img/logo.svg', },
      items: [
        {
          type: 'doc',
          docId: 'introduction',
          position: 'left',
          label: 'Docs',
        },
        { to: '/blog', label: 'Blog', position: 'left' },
        { to: '/help', label: 'Help', position: 'left' },
        {
          href: 'https://bit.ly/2RzPbtn',
          position: 'right',
          className: 'header-slack-link',
        },
        {
          href: 'https://github.com/raystack/optimus',
          className: 'navbar-item-github',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Products',
          items: [
            { label: 'Meteor', href: 'https://github.com/raystack/meteor' },
            { label: 'Firehose', href: 'https://github.com/raystack/firehose' },
            { label: 'Raccoon', href: 'https://github.com/raystack/raccoon' },
            { label: 'Dagger', href: 'https://raystack.github.io/dagger/' },
          ],
        },
        {
          title: 'Resources',
          items: [
            { label: 'Docs', to: '/docs/introduction' },
            { label: 'Blog', to: '/blog', },
            { label: 'Help', to: '/help', },
          ],
        },
        {
          title: 'Community',
          items: [
            { label: 'Slack', href: 'https://bit.ly/2RzPbtn' },
            { label: 'GitHub', href: 'https://github.com/raystack/optimus' }
          ],
        },
      ],
      copyright: `Copyright © 2020-${new Date().getFullYear()} raystack`,
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
    },
    announcementBar: {
      id: 'star-repo',
      content: '⭐️ If you like Optimus, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/raystack/optimus">GitHub</a>! ⭐',
      backgroundColor: '#222',
      textColor: '#eee',
      isCloseable: true,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        gtag: {
          trackingID: 'G-RSCK0YREFM',
          anonymizeIP: true,
        },
        docs: {
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/raystack/optimus/edit/master/docs/',
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/raystack/optimus/edit/master/docs/blog/',
        },
        theme: {
          customCss: [
            require.resolve('./src/css/theme.css'),
            require.resolve('./src/css/custom.css')
          ],
        },
      },
    ],
  ],
};
