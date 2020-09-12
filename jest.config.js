module.exports = {
  rootDir: './',
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
  },
  testRegex: '(/__tests__/.*|(\\.|/)(spec))\\.(jsx?|tsx?)$',
  testEnvironment: 'node',
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  globalSetup: '<rootDir>/jest.setup.js',
  // coverageThreshold: {
  //   global: {
  //     branches: 100,
  //     functions: 100,
  //     lines: 100,
  //     statements: 100,
  //   },
  // },
  reporters: ['default'],
  collectCoverage: true,
  coverageReporters: ['html', 'cobertura'],
};
